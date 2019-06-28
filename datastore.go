package dsbbolt

import (
	"os"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"go.etcd.io/bbolt"
	//	"github.com/ipfs/go-datastore"
)

var (
	defaultBucket                     = []byte("datastore")
	_             datastore.Datastore = (*Datastore)(nil)
)

// Datastore implements an ipfs datastore
// backed by a bbolt db
type Datastore struct {
	db     *bbolt.DB
	bucket []byte
}

// NewDatastore is used to instantiate our datastore
func NewDatastore(path string, opts *bbolt.Options, bucket []byte) (*Datastore, error) {
	db, err := bbolt.Open(path, os.FileMode(0640), nil)
	if err != nil {
		return nil, err
	}
	if bucket == nil {
		bucket = defaultBucket
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucket)
		return err
	}); err != nil {
		return nil, err
	}
	return &Datastore{db, bucket}, nil
}

// Put is used to store something in our underlying datastore
func (d *Datastore) Put(key datastore.Key, value []byte) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Put(key.Bytes(), value)
	})
}

// Delete removes a key/value pair from our datastore
func (d *Datastore) Delete(key datastore.Key) error {
	return d.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(d.bucket).Delete(key.Bytes())
	})
}

// Get is used to retrieve a value from the datastore
func (d *Datastore) Get(key datastore.Key) ([]byte, error) {
	var data []byte
	if err := d.db.View(func(tx *bbolt.Tx) error {
		data = tx.Bucket(d.bucket).Get(key.Bytes())
		return nil
	}); err != nil {
		return nil, err
	}
	return data, nil
}

// Has returns whether the key is present in our datastore
func (d *Datastore) Has(key datastore.Key) (bool, error) {
	data, err := d.Get(key)
	if err != nil {
		return false, err
	}
	return data != nil, nil
}

// GetSize returns the size of the value referenced by key
func (d *Datastore) GetSize(key datastore.Key) (int, error) {
	data, err := d.Get(key)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

// Query performs a complex search query on the underlying datastore
func (d *Datastore) Query(q query.Query) (query.Results, error) {
	return nil, nil
}

// Close is used to close the underlying datastore
func (d *Datastore) Close() error {
	return d.db.Close()
}
