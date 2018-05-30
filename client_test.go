package spanner

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"context"
	"google.golang.org/api/option"
	"cloud.google.com/go/spanner"
	"syscall"
)

type testStruct struct {
	Id string
	Num int64
	Text string
}

var client *spannerClient

func TestMain(m *testing.M) {
	pid,_ := syscall.Getenv("PROJECT_ID")
	id,_ := syscall.Getenv("SPANNER_INSTANCE_ID")
	did,_ := syscall.Getenv("SPANNER_DATABASE_ID")
	ctx := context.Background()

	client, _ = NewSpannerClient(&SpannerClientOptions{
		Context: ctx,
		ProjectID: pid,
		InstanceID: id,
		Db: did,
		Opts: []option.ClientOption{},
	})
}

func TestSpanner(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	ir := testStruct{
		Id: "123456",
		Text: "test text",
		Num: 1234,
	}
	err := client.Insert(ctx, "test", ir)
	assert.NoError(err)

	ir2 := testStruct{}
	s := spanner.Statement{
		SQL : "SELECT * FROM test WHERE Id = @Id",
		Params : map[string]interface{}{
			"Id": ir.Id,
		},
	}
	err = client.FindOne(ctx, s, &ir2)
	assert.NoError(err)
	assert.Equal(ir, ir2)

	err = client.Delete(ctx, "test", spanner.Key{"123456"})
	assert.NoError(err)
}

func TestSpannerClient_InsertOrUpdate(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	ir := testStruct{
		Id: "123456",
		Text: "test text",
		Num: 1234,
	}

	err := client.InsertOrUpdate(ctx, "test", ir)
	assert.NoError(err)

	ir2 := testStruct{}
	s := spanner.Statement{
		SQL : "SELECT * FROM test WHERE Id = @Id",
		Params : map[string]interface{}{
			"Id": ir.Id,
		},
	}
	err = client.FindOne(ctx, s, &ir2)
	assert.NoError(err)
	assert.Equal(ir, ir2)

	ir.Num = 11111
	err = client.InsertOrUpdate(ctx, "test", ir)
	assert.NoError(err)

	ir3 := testStruct{}
	err = client.FindOne(ctx, s, &ir3)
	assert.NoError(err)
	assert.Equal(ir, ir2)
}
