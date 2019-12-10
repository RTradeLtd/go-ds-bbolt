package dsbbolt

import (
	"log"
	"os"

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
	db       *bbolt.DB
	bucket   []byte
	withSync bool
}

// NewDatastore is used to instantiate our datastore
func NewDatastore(path string, opts *bbolt.Options, bucket []byte) (*Datastore, error) {
	if opts == nil {
		opts = bbolt.DefaultOptions
	}
	db, err := bbolt.Open(path, os.FileMode(0640), opts)
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
	return &Datastore{db, bucket, !opts.NoSync}, nil
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
		// taken from https://github.com/ipfs/go-ds-bolt/blob/master/datastore.go#L54
		value := tx.Bucket(d.bucket).Get(key.Bytes())
		if value == nil {
			return datastore.ErrNotFound
		}
		data = make([]byte, len(value))
		copy(data, value)
		return nil
	}); err != nil {
		return nil, err
	}
	return data, nil
}

// Has returns whether the key is present in our datastore
func (d *Datastore) Has(key datastore.Key) (bool, error) {
	var found bool
	err := d.db.View(func(tx *bbolt.Tx) error {
		val := tx.Bucket(d.bucket).Get(key.Bytes())
		found = val != nil
		return nil
	})
	return found, err
}

// GetSize returns the size of the value referenced by key
func (d *Datastore) GetSize(key datastore.Key) (int, error) {
	return datastore.GetBackedSize(d, key)
}

// Query performs a complex search query on the underlying datastore
// For more information see :
// https://github.com/ipfs/go-datastore/blob/aa9190c18f1576be98e974359fd08c64ca0b5a94/examples/fs.go#L96
// https://github.com/etcd-io/bbolt#prefix-scans
func (d *Datastore) Query(q query.Query) (query.Results, error) {
	var (
		orders     = q.Orders
		done       = make(chan bool)
		resultChan = make(chan query.Result)
	)
	log.Printf("%+v\n", q)
	if len(orders) > 0 {
		switch q.Orders[0].(type) {
		case query.OrderByKey, *query.OrderByKey:
			// already ordered by key
			orders = nil
		}
	}
	go func() {
		defer func() {
			done <- true
		}()
		// do some cool search shit here boys
		d.db.View(func(tx *bbolt.Tx) error {
			var (
				buck   = tx.Bucket(d.bucket)
				c      = buck.Cursor()
				prefix []byte
			)
			if q.Prefix != "" {
				prefix = []byte(q.Prefix)
			}
			// handle my sortiness and collect all results up-front
			if len(orders) > 0 {
				var entries []query.Entry
				// query and filter
				for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
					dk := datastore.NewKey(string(k)).String()
					e := query.Entry{Key: dk}

					if !q.KeysOnly {
						// copy afer filtering/sorting
						e.Value = v
						e.Size = len(e.Value)
					}
					if filter(q.Filters, e) {
						continue
					}
					entries = append(entries, e)
				}
				// sort
				query.Sort(orders, entries)
				// offset/limit
				if len(entries) >= q.Offset {
					entries = entries[q.Offset:]
				}
				if q.Limit > 0 && q.Limit < len(entries) {
					entries = entries[:q.Limit]
				}
				/* this is causing issues for some reason
				// offset/limit
				entries = entries[qrb.Query.Offset:]
				if qrb.Query.Limit > 0 {
					if qrb.Query.Limit < len(entries) {
						entries = entries[:qrb.Query.Limit]
					}
				}
				*/
				// send
				for _, e := range entries {
					// copy late so we don't have to copy values we dont use
					e.Value = append(e.Value[0:0:0], e.Value...)
					select {
					case resultChan <- query.Result{Entry: e}:
						// TODO(bonedaddy): we might need to re-enable if this blocks
						//	default:
					}
				}
				return nil
			}
			// Otherwise, send results as we get them.
			offset := 0
			for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
				dk := datastore.NewKey(string(k)).String()
				e := query.Entry{Key: dk, Value: v}
				if !q.KeysOnly {
					// We copy _after_ filtering.
					e.Value = v
					e.Size = len(e.Value)
				}
				// pre-filter
				if filter(q.Filters, e) {
					continue
				}
				// now count this item towards the results
				offset++
				// check the offset
				if offset < q.Offset {
					continue
				}
				e.Value = append(e.Value[0:0:0], e.Value...)
				select {
				case resultChan <- query.Result{Entry: e}:
					offset++
					// TODO(bonedaddy): we might need to re-enable if this blocks
					//	default:
				}
				if q.Limit > 0 && offset >= (q.Offset+q.Limit) {
					// all done.
					return nil
				}
			}
			return nil
		})
	}()
	var entries []query.Entry
	for {
		select {
		case <-done:
			goto FINISHED
		case result := <-resultChan:
			if result.Error != nil {
				log.Println("query result failure: ", result.Error)
			}
			entries = append(entries, result.Entry)
		}
	}
FINISHED:
	return query.ResultsWithEntries(q, entries), nil
}

// Sync is used to manually trigger syncing db contents to disk.
// This call is only usable when synchronous writes aren't enabled
func (d *Datastore) Sync(prefix datastore.Key) error {
	if d.withSync {
		return nil
	}
	return d.db.Sync()
}

// Batch returns a basic batched bolt datastore wrapper
// it is a temporary method until we implement a proper
// transactional batched datastore
func (d *Datastore) Batch() (datastore.Batch, error) {
	tx, err := d.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &bboltBatch{
		tx:  tx,
		bkt: tx.Bucket(d.bucket),
	}, nil
}

// Close is used to close the underlying datastore
func (d *Datastore) Close() error {
	return d.db.Close()
}

// implements batching capabilities
type bboltBatch struct {
	tx  *bbolt.Tx
	bkt *bbolt.Bucket
}

// Commit the underlying batched transactions
func (bb *bboltBatch) Commit() error {
	return bb.tx.Commit()
}

// Add delete operation to the batch
func (bb *bboltBatch) Delete(key datastore.Key) error {
	return bb.bkt.Delete(key.Bytes())
}

// Add a put operation to the batch
func (bb *bboltBatch) Put(key datastore.Key, val []byte) error {
	return bb.bkt.Put(key.Bytes(), val)
}

// filter checks if we should filter out the query.
func filter(filters []query.Filter, entry query.Entry) bool {
	for _, filter := range filters {
		if !filter.Filter(entry) {
			return true
		}
	}
	return false
}
