package kvndb

type DB interface {
	// Put adds or updates entry for given key
	Put(key, value []byte)

	// Get returns value for given key,
	// ErrKeyNotFound if key does not exist
	Get(key []byte) ([]byte, error)

	// Delete removes entry for given key
	Delete(key []byte)

	// Size returns the number of entries
	Size() uint64

	// Keys returns a channel that will iterate
	// over keys of all entries
	Keys() <-chan []byte

	// KeysAndValues returns a channel that will iterate
	// over all keys and values of all entries
	KeysAndValues() <-chan *Tuple
}

type Tuple struct {
	Key   []byte
	Value []byte
}
