package server

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestPartitionQuery_ReturnsEmptyPartitions(t *testing.T) {
	s := newTestServer(t)
	resp, err := s.PartitionQuery(context.Background(), &firestorepb.PartitionQueryRequest{
		Parent: collectionParent(),
		QueryType: &firestorepb.PartitionQueryRequest_StructuredQuery{
			StructuredQuery: &firestorepb.StructuredQuery{
				From: []*firestorepb.StructuredQuery_CollectionSelector{
					{CollectionId: "things"},
				},
			},
		},
		PartitionCount: 10,
	})
	if err != nil {
		t.Fatalf("PartitionQuery: %v", err)
	}
	if len(resp.Partitions) != 0 {
		t.Errorf("want 0 partitions (single-shard emulator), got %d", len(resp.Partitions))
	}
	if resp.NextPageToken != "" {
		t.Errorf("want empty NextPageToken, got %q", resp.NextPageToken)
	}
}
