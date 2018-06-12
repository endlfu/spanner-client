package spanner

import (
	"testing"
	"context"
	"github.com/stretchr/testify/assert"
	"cloud.google.com/go/spanner"
	"syscall"
	"google.golang.org/api/option"
)

type testStruct struct {
	Id string
	Num int64
	Text string
}

func TestSpanner(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	client := newClient(ctx)
	client.Truncate(ctx, []string{"test"})

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

	client := newClient(ctx)
	client.Truncate(ctx, []string{"test"})

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
	assert.Equal(ir, ir3)
}

func newClient(ctx context.Context) *spannerClient {
	pid,_ := syscall.Getenv("PROJECT_ID")
	id,_ := syscall.Getenv("SPANNER_INSTANCE_ID")
	did,_ := syscall.Getenv("SPANNER_DATABASE_ID")

	client, _ := NewSpannerClient(&SpannerClientOptions{
		Context: ctx,
		ProjectID: pid,
		InstanceID: id,
		Db: did,
		Opts: []option.ClientOption{},
	})

	return client
}
