package datastore

import (
	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// messageFieldSize is the wire size of a present length-delimited message field.
func messageFieldSize(number protowire.Number, size int) int {
	return protowire.SizeTag(number) + protowire.SizeBytes(size)
}

// queryResponseSize avoids walking accumulated entities when envelope fields change.
func queryResponseSize(response *datastorepb.RunQueryResponse, resultBytes int) int {
	results := response.Batch.EntityResults
	response.Batch.EntityResults = nil
	batchBytes := proto.Size(response.Batch)
	envelopeBytes := proto.Size(response) - messageFieldSize(1, batchBytes)
	response.Batch.EntityResults = results
	return envelopeBytes + messageFieldSize(1, batchBytes+resultBytes)
}
