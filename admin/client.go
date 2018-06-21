package admin

import (
	"fmt"
	"log"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"context"
)

var (
	admin *database.DatabaseAdminClient
)

type SpannerAdminDB struct {
	parent string
	client *database.DatabaseAdminClient
}

func NewAdminClient(ctx context.Context, projectID string, instansID string) *SpannerAdminDB {
	client, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatalf("cannot create admin client: %v", err)
	}

	return &SpannerAdminDB{
		client: client,
		parent: fmt.Sprintf("projects/%s/instances/%s", projectID, instansID),
	}
}

func (s *SpannerAdminDB) createDatabase(ctx context.Context, dbName string, stmts []string) {
	dbPath := fmt.Sprintf("%s/databases/%s", s.parent, dbName)

	op, err := s.client.CreateDatabase(ctx, &adminpb.CreateDatabaseRequest{
		Parent:          s.parent,
		CreateStatement: "CREATE DATABASE " + dbName,
		ExtraStatements: stmts,
	})
	if err != nil {
		log.Fatalf("cannot create testing DB %s: %v", dbPath, err)
	}
	if _, err := op.Wait(ctx); err != nil {
		log.Fatalf("cannot create testing DB %s: %v", dbPath, err)
	}
}

func (s *SpannerAdminDB) DropDatabase(ctx context.Context, dbName string) {
	dbPath := fmt.Sprintf("%s/databases/%s", s.parent, dbName)

	if err := s.client.DropDatabase(ctx, &adminpb.DropDatabaseRequest{Database: dbPath}); err != nil {
		log.Printf("failed to drop database %s (error %v), might need a manual removal",
			dbPath, err)
	}
}
