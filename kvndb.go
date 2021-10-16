package kvndb

import (
	"encoding/hex"
	"sync"
)

const (
	maxHistory uint = 999_999
)

type DB interface {
	// Put adds or updates entry for given key.
	Put(key, value []byte) error

	// Get returns value for given key, ErrKeyNotFound if key
	// does not exist.
	Get(key []byte) ([]byte, error)

	// Delete removes entry for given key.
	Delete(key []byte) error

	// Size returns the number of currently stored entries.
	Size() uint64

	// Keys returns a channel that will iterate	over keys of all
	// entries.This operation is synchronous, which means all
	// other operations will be	blocked until all values are read.
	// You MUST read all values until the channel is closed. Best
	// to use `range`.
	Keys() (<-chan []byte, error)

	// KeysAndValues returns a channel that will iterate
	// over all keys and values of all entries. This operation
	// is synchronous, which means all other operations will be
	// blocked until all values are read. You MUST read all values
	// until the channel is closed. Best to use `range`.
	KeysAndValues() (<-chan *Tuple, error)

	// Save will write a snapshot of data into provided
	// directory path. If snapshot successful it will clean up
	// keeping only `hist` number of snapshots. This operation
	// is synchronous, which means all other operations will be
	// blocked until it is done. `hist` value of 0 will only
	// save current copy. Value of 1 will keep current and previous.
	Save(dir string, hist uint) error

	// Load will load data from snapshot. It will replace any
	// current data completely (not merge/update). It will
	// always load latest found snapshot version. This operation
	// is synchronous, which means all other operations will be
	// blocked until it is done.
	Load(dir string) error

	// Wait will block until a previously started operation frees
	// mutex. If datastore was already closed, it is a no-op.
	Wait()

	// Close reset the data store and set status to closed. After
	// this no operations can be done.
	Close() error
}

type Tuple struct {
	Key   []byte
	Value []byte
}

type db struct {
	data     map[string][]byte
	mutex    *sync.Mutex
	isClosed bool
}

func (d *db) Put(key, value []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isClosed {
		return ErrAlreadyClosed
	}

	d.data[hex.EncodeToString(key)] = value

	return nil
}

func (d *db) Get(key []byte) ([]byte, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isClosed {
		return nil, ErrAlreadyClosed
	}

	value, ok := d.data[hex.EncodeToString(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

func (d *db) Delete(key []byte) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isClosed {
		return ErrAlreadyClosed
	}

	delete(d.data, hex.EncodeToString(key))

	return nil
}

func (d *db) Size() uint64 {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return uint64(len(d.data))
}

func (d *db) Keys() (<-chan []byte, error) {
	d.mutex.Lock()

	if d.isClosed {
		return nil, ErrAlreadyClosed
	}

	ch := make(chan []byte)

	go func() {
		defer d.mutex.Unlock()
		for key := range d.data {
			ch <- hexToBytes(key)
		}
		close(ch)
	}()

	return ch, nil
}

func (d *db) KeysAndValues() (<-chan *Tuple, error) {
	d.mutex.Lock()

	if d.isClosed {
		return nil, ErrAlreadyClosed
	}

	ch := make(chan *Tuple)

	go func() {
		defer d.mutex.Unlock()
		for key, val := range d.data {
			ch <- &Tuple{
				Key:   hexToBytes(key),
				Value: val,
			}
		}
		close(ch)
	}()

	return ch, nil
}

func (d *db) Save(dir string, hist uint) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isClosed {
		return ErrAlreadyClosed
	}

	if hist > maxHistory {
		return ErrTooMuchHistory
	}

	return save(d, dir, hist)
}

func (d *db) Load(dir string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isClosed {
		return ErrAlreadyClosed
	}

	return load(d, dir)
}

func (d *db) Wait() {
	d.mutex.Lock()
	defer d.mutex.Unlock()
}

func (d *db) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.isClosed {
		return ErrAlreadyClosed
	}

	d.data = nil
	d.isClosed = true

	return nil
}

func New() DB {
	return newDb()
}

func newDb() *db {
	return &db{
		data:     make(map[string][]byte),
		mutex:    &sync.Mutex{},
		isClosed: false,
	}
}
