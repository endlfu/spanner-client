package admin

import (
	"testing"
	"context"
	"syscall"
)

func TestAdminSpanner(t *testing.T) {
	ctx := context.Background()

	pid,_ := syscall.Getenv("PROJECT_ID")
	id,_ := syscall.Getenv("SPANNER_INSTANCE_ID")
	client := NewAdminClient(ctx, pid, id)

	client.createDatabase(ctx, "test1234", []string{})
	client.DropDatabase(ctx, "test1234")
}

