package dsbbolt

import (
	"fmt"
	"os"

	"bytes"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"go.etcd.io/bbolt"
	//	"github.com/ipfs/go-datastore"
)

var (
	defaultBucket                    = []byte("datastore")
	_             datastore.Batching = (*Datastore)(nil)
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
// For more information see :
// https://github.com/ipfs/go-datastore/blob/aa9190c18f1576be98e974359fd08c64ca0b5a94/examples/fs.go#L96
// https://github.com/boltdb/bolt/issues/518#issuecomment-187211346
func (d *Datastore) Query(q query.Query) (query.Results, error) {
	resBuilder := query.NewResultBuilder(q)
	if err := d.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(d.bucket).Cursor()
		if q.Prefix == "" {
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				result := query.Result{}
				key := datastore.NewKey(fmt.Sprintf("%v", k))
				result.Entry.Key = key.String()
				if !q.KeysOnly {
					result.Entry.Value, result.Error = d.Get(key)
				}
				select {
				case resBuilder.Output <- result:
				default:
					continue
				}
			}
			return nil
		}
		pref := []byte(q.Prefix)
		for k, _ := cursor.Seek(pref); bytes.HasPrefix(k, pref); k, _ = cursor.Next() {
			result := query.Result{}
			key := datastore.NewKey(fmt.Sprintf("%v", k))
			result.Entry.Key = key.String()
			if !q.KeysOnly {
				result.Entry.Value, result.Error = d.Get(key)
			}
			// initiate a non-blocking channel send
			select {
			case resBuilder.Output <- result:
			default:
				continue
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// close the result builder since we are done using it
	resBuilder.Process.Close()
	return resBuilder.Results(), nil
}

// Batch returns a basic batched bolt datastore wrapper
// it is a temporary method until we implement a proper
// transactional batched datastore
func (d *Datastore) Batch() (datastore.Batch, error) {
	return datastore.NewBasicBatch(d), nil
}

// Close is used to close the underlying datastore
func (d *Datastore) Close() error {
	return d.db.Close()
}
