package spanner

import (
	"context"
	"fmt"
	"reflect"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/endlfu/spanner_client/errors"
)

type spannerClient struct {
	client  *spanner.Client
	options *SpannerClientOptions
}

type SpannerClientOptions struct {
	Context    context.Context
	ProjectID  string
	InstanceID string
	Db         string
	Opts       []option.ClientOption
}

const (
	databaseTemplate = "projects/%s/instances/%s/databases/%s"
)

func NewSpannerClient(opt *SpannerClientOptions) (*spannerClient, error) {
	db := fmt.Sprintf(databaseTemplate, opt.ProjectID, opt.ProjectID, opt.Db)
	c, err := spanner.NewClient(opt.Context, db, opt.Opts...)

	if err != nil {
		return nil, err
	}

	return &spannerClient{
		client: c,
		options: opt,
	}, nil
}

func (c spannerClient) Find(ctx context.Context, statement spanner.Statement, dst interface{}, limit uint) ([]interface{}, error) {
	rows := c.client.ReadOnlyTransaction().Query(ctx, statement)
	defer rows.Stop()

	rs := []interface{}{}
	err := rows.Do(func(r *spanner.Row) error {
		s := r.ToStruct(dst)
		rs = append(rs, s)

		if uint(len(rs)) >= limit {
			rows.Stop()
		}

		return nil
	})

	if err != nil {
		return nil, errors.NewClientError(err.Error())
	}

	return rs, nil
}

func (c spannerClient) FindOne(ctx context.Context, statement spanner.Statement, dst interface{}) error {
	rows := c.client.ReadOnlyTransaction().Query(ctx, statement)
	defer rows.Stop()

	r, err := rows.Next()
	if err == iterator.Done {
		return errors.NewNotFoundError("data not found")
	}

	if err != nil {
		return err
	}

	// An error is returned When multiple results
	_, err = rows.Next()
	if err != iterator.Done {
		return status.Error(codes.Aborted, "multiple data found")
	}

	err = r.ToStruct(dst)
	return err
}

func (c spannerClient) Update(ctx context.Context, tableName string, ir interface{}) error {
	cols, err := getColsFromStruct(ir)
	if err != nil {
		return err
	}

	v := reflect.ValueOf(ir)

	vals := []interface{}{}
	for _, e := range cols {
		vals = append(vals, v.FieldByName(e).Interface())
	}

	_, err = c.client.Apply(ctx, []*spanner.Mutation{
		spanner.Update(tableName, cols, vals),
	})

	if err != nil {
		return errors.NewClientError(err.Error())
	}

	return nil
}

func (c spannerClient) Apply(ctx context.Context, tableName string, mutation []*spanner.Mutation) error {
	_, err := c.client.Apply(ctx, mutation)

	if err != nil {
		return errors.NewClientError(err.Error())
	}

	return nil
}

func (c spannerClient) Insert(ctx context.Context, tableName string, ir interface{}) error {
	cols, err := getColsFromStruct(ir)
	if err != nil {
		return errors.NewInvalidStructError(err.Error())
	}

	vals, err := getValuesFromStruct(ir, cols)
	if err != nil {
		return err
	}

	_, err = c.client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert(tableName, cols, vals),
	})

	if err != nil {
		return errors.NewClientError(err.Error())
	}

	return nil
}

func (c spannerClient) InsertOrUpdate(ctx context.Context, tableName string, ir interface{}) error {
	cols, err := getColsFromStruct(ir)
	if err != nil {
		return errors.NewInvalidStructError(err.Error())
	}

	vals, err := getValuesFromStruct(ir, cols)
	if err != nil {
		return err
	}

	_, err = c.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate(tableName, cols, vals),
	})

	if err != nil {
		return errors.NewClientError(err.Error())
	}

	return nil
}

func (c spannerClient) Delete(ctx context.Context, tableName string, key spanner.Key) error {
	keys := []spanner.Key{key}
	return c.DeleteMulti(ctx, tableName, keys)
}

func (c spannerClient) DeleteMulti(ctx context.Context, table string, keys []spanner.Key) error {
	ms := make([]*spanner.Mutation, 0, len(keys))
	for _, key := range keys {
		ms = append(ms, spanner.Delete(table, key))
	}

	_, err := c.client.Apply(ctx, ms)
	return err
}

func (c spannerClient) Truncate(ctx context.Context, tableNames []string) error {
	var m []*spanner.Mutation
	for _, table := range tableNames {
		m = append(m, spanner.Delete(table, spanner.AllKeys()))
	}

	_, err := c.client.ReadWriteTransaction(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			return txn.BufferWrite(m)
		})

	if err != nil {
		return err
	}
	return nil
}

func getColsFromStruct(src interface{}) ([]string, error) {
	reflectStruct := reflect.ValueOf(src).Type()

	if reflectStruct.Kind() == reflect.Ptr {
		reflectStruct = reflectStruct.Elem()
	}

	if dataType := reflectStruct.Kind(); dataType != reflect.Struct {
		return nil, fmt.Errorf("Unsupported data type %s", dataType.String())
	}
	var cols []string
	for i := 0; i < reflectStruct.NumField(); i++ {
		cols = append(cols, reflectStruct.Field(i).Name)
	}

	return cols, nil
}

func getValuesFromStruct(src interface{}, cols []string) ([]interface{}, error) {
	v := reflect.Indirect(reflect.ValueOf(src))

	vals := []interface{}{}
	for _, e := range cols {
		if v.FieldByName(e).CanInterface() {
			vals = append(vals, reflect.Indirect(v.FieldByName(e)).Interface())
		} else {
			msg := "ir is invalid, first character of the identifier's name is a Unicode upper case letter"
			return nil, errors.NewInvalidStructError(msg)
		}
	}

	return vals, nil
}


