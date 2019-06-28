package dsbbolt

import (
	"fmt"
	"testing"

	"reflect"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

func Test_NewDatastore(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"Success", args{"./tmp"}, false},
		{"Fail", args{"/root/toor"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if ds, err := NewDatastore(tt.args.path, nil, nil); (err != nil) != tt.wantErr {
				t.Fatalf("NewDatastore() err = %v, wantErr %v", err, tt.wantErr)
			} else if !tt.wantErr {
				if err := ds.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func Test_Datastore(t *testing.T) {
	ds, err := NewDatastore("./tmp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	key := datastore.NewKey("keks")
	if err := ds.Put(key, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	data, err := ds.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(data, []byte("hello world")) {
		t.Fatal("bad data")
	}
	if has, err := ds.Has(key); err != nil {
		t.Fatal(err)
	} else if !has {
		t.Fatal("should have key")
	}
	if size, err := ds.GetSize(key); err != nil {
		t.Fatal(err)
	} else if size != len([]byte("hello world")) {
		t.Fatal("incorrect data size")
	}

	rs, err := ds.Query(query.Query{Prefix: key.String()})
	if err != nil {
		t.Fatal(err)
	}
	res, err := rs.Rest()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range res {
		fmt.Printf("%+v\n", v)
	}
	if err := ds.Delete(key); err != nil {
		t.Fatal(err)
	}
	if has, err := ds.Has(key); err != nil {
		t.Fatal(err)
	} else if has {
		t.Fatal("should not have key")
	}
	if size, err := ds.GetSize(key); err != nil {
		t.Fatal(err)
	} else if size != 0 {
		t.Fatal("bad size")
	}

}
