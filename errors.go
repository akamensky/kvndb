package kvndb

import "errors"

var (
	ErrKeyNotFound      = errors.New("kvndb: key not found")
	ErrTooMuchHistory   = errors.New("kvndb: do you really need that much history")
	ErrSnapshotNotFound = errors.New("kvndb: there are no loadable snapshots, data was reset")
)
