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
	Put(key, value []byte)

	// Get returns value for given key, ErrKeyNotFound if key
	// does not exist.
	Get(key []byte) ([]byte, error)

	// Delete removes entry for given key.
	Delete(key []byte)

	// Size returns the number of currently stored entries.
	Size() uint64

	// Keys returns a channel that will iterate	over keys of all
	// entries.This operation is synchronous, which means all
	// other operations will be	blocked until all values are read.
	// You MUST read all values until the channel is closed. Best
	// to use `range`.
	Keys() <-chan []byte

	// KeysAndValues returns a channel that will iterate
	// over all keys and values of all entries. This operation
	// is synchronous, which means all other operations will be
	// blocked until all values are read. You MUST read all values
	// until the channel is closed. Best to use `range`.
	KeysAndValues() <-chan *Tuple

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
}

type Tuple struct {
	Key   []byte
	Value []byte
}

type db struct {
	data  map[string][]byte
	mutex *sync.Mutex
}

func (d *db) Put(key, value []byte) {
	d.mutex.Lock()
	d.data[hex.EncodeToString(key)] = value
	d.mutex.Unlock()
}

func (d *db) Get(key []byte) ([]byte, error) {
	d.mutex.Lock()
	value, ok := d.data[hex.EncodeToString(key)]
	d.mutex.Unlock()
	if !ok {
		return nil, ErrKeyNotFound
	}

	return value, nil
}

func (d *db) Delete(key []byte) {
	d.mutex.Lock()
	delete(d.data, hex.EncodeToString(key))
	d.mutex.Unlock()
}

func (d *db) Size() uint64 {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return uint64(len(d.data))
}

func (d *db) Keys() <-chan []byte {
	d.mutex.Lock()
	ch := make(chan []byte)

	go func() {
		defer d.mutex.Unlock()
		for key := range d.data {
			ch <- hexToBytes(key)
		}
	}()

	return ch
}

func (d *db) KeysAndValues() <-chan *Tuple {
	d.mutex.Lock()
	ch := make(chan *Tuple)

	go func() {
		defer d.mutex.Unlock()
		for key, val := range d.data {
			ch <- &Tuple{
				Key:   hexToBytes(key),
				Value: val,
			}
		}
	}()

	return ch
}

func (d *db) Save(dir string, hist uint) error {
	if hist > maxHistory {
		return ErrTooMuchHistory
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return save(d, dir, hist)
}

func (d *db) Load(dir string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return load(d, dir)
}

func New() DB {
	return newDb()
}

func newDb() *db {
	return &db{
		data:  make(map[string][]byte),
		mutex: &sync.Mutex{},
	}
}
