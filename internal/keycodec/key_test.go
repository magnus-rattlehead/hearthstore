package keycodec

import (
	"testing"

	datastorepb "cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

func FuzzPathRoundTrip(f *testing.F) {
	f.Add("a/b", "1", int64(1))
	f.Add("零", "a\x00/b", int64(-1))
	f.Fuzz(func(t *testing.T, kind, name string, id int64) {
		if kind == "" || name == "" {
			return
		}
		parts := []*datastorepb.Key_PathElement{
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Id{Id: id}},
			{Kind: kind, IdType: &datastorepb.Key_PathElement_Name{Name: name}},
		}
		decoded, err := ParsePath(Path(parts))
		if err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(&datastorepb.Key{Path: parts}, &datastorepb.Key{Path: decoded}) {
			t.Fatal("key path did not round-trip")
		}
	})
}
