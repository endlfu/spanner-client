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
	database_templete = "projects/%s/instances/%s/databases/%s"
)

func NewSpannerClient(opt *SpannerClientOptions) (*spannerClient, error) {
	db := fmt.Sprintf(database_templete, opt.ProjectID, opt.ProjectID, opt.Db)
	c, err := spanner.NewClient(opt.Context, db, opt.Opts...)

	if err != nil {
		return nil, err
	}

	return &spannerClient{
		c,
		opt,
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

	// 複数の結果が返ってきたときはエラーを返す
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

func (c spannerClient) Insert(ctx context.Context, tableName string, ir interface{}) error {
	cols, err := getColsFromStruct(ir)
	if err != nil {
		return errors.NewInvalidStructError(err.Error())
	}

	v := reflect.ValueOf(ir)

	vals := []interface{}{}
	for _, e := range cols {
		if v.FieldByName(e).CanInterface() {
			vals = append(vals, v.FieldByName(e).Interface())
		} else {
			msg := "ir is invalid, first character of the identifier's name is a Unicode upper case letter"
			return errors.NewInvalidStructError(msg)
		}
	}

	_, err = c.client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert(tableName, cols, vals),
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
