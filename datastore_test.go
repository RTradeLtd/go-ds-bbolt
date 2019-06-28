package dsbbolt

import (
	"testing"

	"reflect"

	"github.com/ipfs/go-datastore"
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
			if ds, err := NewDatastore(tt.args.path, nil); (err != nil) != tt.wantErr {
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
	ds, err := NewDatastore("./tmp", nil)
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
	if err := ds.Delete(key); err != nil {
		t.Fatal(err)
	}
}
