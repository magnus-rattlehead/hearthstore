package datastore

import (
	"context"
	"testing"
	"time"

	longrunningpb "cloud.google.com/go/longrunning/autogen/longrunningpb"
)

func TestWaitOperationReturnsWhenOperationChanges(t *testing.T) {
	server := &IndexOperationsServer{}
	server.put(&longrunningpb.Operation{Name: "build"})

	result := make(chan *longrunningpb.Operation, 1)
	errs := make(chan error, 1)
	go func() {
		op, err := server.WaitOperation(context.Background(), &longrunningpb.WaitOperationRequest{Name: "build"})
		result <- op
		errs <- err
	}()
	server.put(&longrunningpb.Operation{Name: "build", Done: true})

	select {
	case op := <-result:
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		if !op.Done {
			t.Fatal("WaitOperation returned before the completion event")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitOperation did not react to the completion event")
	}
}
