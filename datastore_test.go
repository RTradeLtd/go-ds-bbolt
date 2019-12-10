package dsbbolt

import (
	"fmt"
	"os"
	"testing"

	"reflect"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	dstest "github.com/ipfs/go-datastore/test"
	"go.etcd.io/bbolt"
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

func Test_Batch(t *testing.T) {
	defer os.RemoveAll("./tmp")
	ds, err := NewDatastore("./tmp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	batcher, err := ds.Batch()
	if err != nil {
		t.Fatal(err)
	}
	if err := batcher.Put(datastore.NewKey("helloworld"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := batcher.Delete(datastore.NewKey("helloworld")); err != nil {
		t.Fatal(err)
	}
	batcher.Commit()
	if _, err := ds.Get(datastore.NewKey("helloworld")); err == nil {
		t.Fatal("error expected")
	}
}

func Test_Sync(t *testing.T) {
	type args struct {
		sync bool
	}
	tests := []struct {
		name string
		args args
	}{
		{"With-Sync", args{true}},
		{"Without-Sync", args{false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer os.RemoveAll("./tmp")
			opts := bbolt.DefaultOptions
			// we want the "inverse" of sync because this is "no sync"
			// that is, if we specify sync, we want to say "no thank you nosync"
			opts.NoSync = !tt.args.sync
			ds, err := NewDatastore("./tmp", opts, nil)
			if err != nil {
				t.Fatal(err)
			}
			if ds.withSync != tt.args.sync {
				t.Fatal("bad sync status")
			}
			if err := ds.Sync(datastore.NewKey("hmm")); err != nil {
				t.Fatal(err)
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
	key2 := datastore.NewKey("keks2")
	if err := ds.Put(key, []byte("hello world")); err != nil {
		t.Fatal(err)
	}
	if err := ds.Put(key2, []byte("hello world")); err != nil {
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
	// test a query where we specify a search key
	rs, err := ds.Query(query.Query{Prefix: key.String()})
	if err != nil {
		t.Fatal(err)
	}
	res, err := rs.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		fmt.Printf("only found %v results \n", len(res))
		for _, v := range res {
			fmt.Printf("%+v\n", v)
		}
		t.Fatal("bad number of results")
	}
	// test a query where we dont specify a search key
	rs, err = ds.Query(query.Query{})
	if err != nil {
		t.Fatal(err)
	}
	res, err = rs.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("bad number of results")
	}
	// test a query where we specify a partial prefix
	rs, err = ds.Query(query.Query{Prefix: "/kek"})
	if err != nil {
		t.Fatal(err)
	}
	res, err = rs.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("bad number of results")
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

func TestSuite(t *testing.T) {
	defer os.RemoveAll("./tmp")
	db, err := NewDatastore("./tmp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dstest.SubtestAll(t, db)
}
