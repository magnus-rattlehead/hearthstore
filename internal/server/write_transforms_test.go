package server

import (
	"context"
	"testing"

	firestorepb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func seedDoc(t *testing.T, s *Server, name string, fields map[string]*firestorepb.Value) {
	t.Helper()
	ctx := context.Background()
	_, err := s.Commit(ctx, &firestorepb.CommitRequest{
		Database: "projects/" + testProject + "/databases/" + testDB,
		Writes: []*firestorepb.Write{
			{Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name, Fields: fields}}},
		},
	})
	if err != nil {
		t.Fatalf("Commit seed: %v", err)
	}
}

func getDocFields(t *testing.T, s *Server, name string) map[string]*firestorepb.Value {
	t.Helper()
	doc, err := s.GetDocument(context.Background(), &firestorepb.GetDocumentRequest{Name: name})
	if err != nil {
		t.Fatalf("GetDocument %s: %v", name, err)
	}
	return doc.Fields
}

func commit(t *testing.T, s *Server, writes ...*firestorepb.Write) {
	t.Helper()
	_, err := s.Commit(context.Background(), &firestorepb.CommitRequest{
		Database: "projects/" + testProject + "/databases/" + testDB,
		Writes:   writes,
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestWriteTransform_SetToServerValue(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "ts1")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"x": intVal(1)})

	commit(t, s, &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
		},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{
			{
				FieldPath: "updatedAt",
				TransformType: &firestorepb.DocumentTransform_FieldTransform_SetToServerValue{
					SetToServerValue: firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME,
				},
			},
		},
	})

	fields := getDocFields(t, s, name)
	if fields["updatedAt"] == nil {
		t.Error("updatedAt should be set by SERVER_TIMESTAMP transform")
	}
	if _, ok := fields["updatedAt"].ValueType.(*firestorepb.Value_TimestampValue); !ok {
		t.Errorf("updatedAt should be a timestamp, got %T", fields["updatedAt"].ValueType)
	}
}

func TestWriteTransform_Increment(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "inc1")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"n": intVal(5)})

	commit(t, s, &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"n": intVal(5)}},
		},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{
			{
				FieldPath:     "n",
				TransformType: &firestorepb.DocumentTransform_FieldTransform_Increment{Increment: intVal(3)},
			},
		},
	})

	if n := getDocFields(t, s, name)["n"].GetIntegerValue(); n != 8 {
		t.Errorf("n = %d, want 8", n)
	}
}

func TestWriteTransform_Maximum(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "max1")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"v": intVal(10)})

	commit(t, s, &firestorepb.Write{
		Operation:        &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"v": intVal(10)}}},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{{FieldPath: "v", TransformType: &firestorepb.DocumentTransform_FieldTransform_Maximum{Maximum: intVal(5)}}},
	})
	if v := getDocFields(t, s, name)["v"].GetIntegerValue(); v != 10 {
		t.Errorf("v = %d, want 10 (max(10,5))", v)
	}

	commit(t, s, &firestorepb.Write{
		Operation:        &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"v": intVal(10)}}},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{{FieldPath: "v", TransformType: &firestorepb.DocumentTransform_FieldTransform_Maximum{Maximum: intVal(20)}}},
	})
	if v := getDocFields(t, s, name)["v"].GetIntegerValue(); v != 20 {
		t.Errorf("v = %d, want 20 (max(10,20))", v)
	}
}

func TestWriteTransform_Minimum(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "min1")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"v": intVal(10)})

	commit(t, s, &firestorepb.Write{
		Operation:        &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"v": intVal(10)}}},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{{FieldPath: "v", TransformType: &firestorepb.DocumentTransform_FieldTransform_Minimum{Minimum: intVal(15)}}},
	})
	if v := getDocFields(t, s, name)["v"].GetIntegerValue(); v != 10 {
		t.Errorf("v = %d, want 10 (min(10,15))", v)
	}

	commit(t, s, &firestorepb.Write{
		Operation:        &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"v": intVal(10)}}},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{{FieldPath: "v", TransformType: &firestorepb.DocumentTransform_FieldTransform_Minimum{Minimum: intVal(3)}}},
	})
	if v := getDocFields(t, s, name)["v"].GetIntegerValue(); v != 3 {
		t.Errorf("v = %d, want 3 (min(10,3))", v)
	}
}

func TestWriteTransform_AppendMissingElements(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "arr1")
	arr := func(vals ...*firestorepb.Value) *firestorepb.Value {
		return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: vals}}}
	}
	seedDoc(t, s, name, map[string]*firestorepb.Value{"tags": arr(strVal("a"), strVal("b"))})

	commit(t, s, &firestorepb.Write{
		Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{
			"tags": arr(strVal("a"), strVal("b")),
		}}},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{{
			FieldPath: "tags",
			TransformType: &firestorepb.DocumentTransform_FieldTransform_AppendMissingElements{
				AppendMissingElements: &firestorepb.ArrayValue{Values: []*firestorepb.Value{strVal("b"), strVal("c")}},
			},
		}},
	})

	tags := getDocFields(t, s, name)["tags"].GetArrayValue().GetValues()
	if len(tags) != 3 {
		t.Errorf("want 3 tags (a,b,c), got %d", len(tags))
	}
}

func TestWriteTransform_RemoveAllFromArray(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "arr2")
	arr := func(vals ...*firestorepb.Value) *firestorepb.Value {
		return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{ArrayValue: &firestorepb.ArrayValue{Values: vals}}}
	}
	seedDoc(t, s, name, map[string]*firestorepb.Value{"tags": arr(strVal("a"), strVal("b"), strVal("c"))})

	commit(t, s, &firestorepb.Write{
		Operation: &firestorepb.Write_Update{Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{
			"tags": arr(strVal("a"), strVal("b"), strVal("c")),
		}}},
		UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{{
			FieldPath: "tags",
			TransformType: &firestorepb.DocumentTransform_FieldTransform_RemoveAllFromArray{
				RemoveAllFromArray: &firestorepb.ArrayValue{Values: []*firestorepb.Value{strVal("a"), strVal("c")}},
			},
		}},
	})

	tags := getDocFields(t, s, name)["tags"].GetArrayValue().GetValues()
	if len(tags) != 1 || tags[0].GetStringValue() != "b" {
		t.Errorf("want [b], got %v", tags)
	}
}

func TestUpdateMask_NestedFieldPath(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "nested1")

	mapVal := func(city, zip string) *firestorepb.Value {
		return &firestorepb.Value{
			ValueType: &firestorepb.Value_MapValue{
				MapValue: &firestorepb.MapValue{
					Fields: map[string]*firestorepb.Value{
						"city": strVal(city),
						"zip":  strVal(zip),
					},
				},
			},
		}
	}

	// Seed with address.city="NYC", address.zip="10001"
	seedDoc(t, s, name, map[string]*firestorepb.Value{"address": mapVal("NYC", "10001")})

	// Update only address.city → "LA", leaving address.zip intact.
	commit(t, s, &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{
				Name: name,
				Fields: map[string]*firestorepb.Value{
					"address": {
						ValueType: &firestorepb.Value_MapValue{
							MapValue: &firestorepb.MapValue{
								Fields: map[string]*firestorepb.Value{"city": strVal("LA")},
							},
						},
					},
				},
			},
		},
		UpdateMask: &firestorepb.DocumentMask{FieldPaths: []string{"address.city"}},
	})

	fields := getDocFields(t, s, name)
	addr := fields["address"].GetMapValue().GetFields()
	if addr["city"].GetStringValue() != "LA" {
		t.Errorf("address.city = %q, want LA", addr["city"].GetStringValue())
	}
	if addr["zip"].GetStringValue() != "10001" {
		t.Errorf("address.zip = %q, want 10001 (should be preserved)", addr["zip"].GetStringValue())
	}
}

func TestWriteTransform_LegacyWriteTransform(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "legacy1")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"n": intVal(10)})

	// Legacy Write_Transform (DocumentTransform, not UpdateTransforms).
	commit(t, s, &firestorepb.Write{
		Operation: &firestorepb.Write_Transform{
			Transform: &firestorepb.DocumentTransform{
				Document: name,
				FieldTransforms: []*firestorepb.DocumentTransform_FieldTransform{{
					FieldPath:     "n",
					TransformType: &firestorepb.DocumentTransform_FieldTransform_Increment{Increment: intVal(5)},
				}},
			},
		},
	})

	if n := getDocFields(t, s, name)["n"].GetIntegerValue(); n != 15 {
		t.Errorf("n = %d, want 15", n)
	}
}

// TestWriteTransform_TransformResults verifies that WriteResult.TransformResults is populated
// with the server-computed values for each field transform. The Firebase JS SDK asserts
// fieldTransforms.length === serverTransformResults.length in applyToRemoteDocument; a missing
// TransformResults causes an "Unexpected state" assertion failure and breaks the AsyncQueue.
func TestWriteTransform_TransformResults(t *testing.T) {
	s := newTestServer(t)
	name := docName("things", "tr1")
	seedDoc(t, s, name, map[string]*firestorepb.Value{"x": intVal(1)})

	resp, err := s.Commit(context.Background(), &firestorepb.CommitRequest{
		Database: "projects/" + testProject + "/databases/" + testDB,
		Writes: []*firestorepb.Write{
			{
				Operation: &firestorepb.Write_Update{
					Update: &firestorepb.Document{Name: name, Fields: map[string]*firestorepb.Value{"x": intVal(1)}},
				},
				UpdateTransforms: []*firestorepb.DocumentTransform_FieldTransform{
					{
						FieldPath: "updatedAt",
						TransformType: &firestorepb.DocumentTransform_FieldTransform_SetToServerValue{
							SetToServerValue: firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME,
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if len(resp.WriteResults) != 1 {
		t.Fatalf("want 1 WriteResult, got %d", len(resp.WriteResults))
	}
	wr := resp.WriteResults[0]
	if len(wr.TransformResults) != 1 {
		t.Fatalf("want 1 TransformResult (for serverTimestamp), got %d", len(wr.TransformResults))
	}
	if _, ok := wr.TransformResults[0].ValueType.(*firestorepb.Value_TimestampValue); !ok {
		t.Errorf("TransformResults[0] should be a timestamp, got %T", wr.TransformResults[0].ValueType)
	}
}
