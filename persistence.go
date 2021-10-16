package kvndb

import (
	"encoding/hex"
	"io"
)

func save(d *db, dir string, hist uint) error {
	maxId, err := getMaxSnapshotId(dir)
	if err != nil {
		return err
	}

	id := maxId + 1

	fd, err := getSnapshotFDForWriting(id, dir)
	if err != nil {
		return err
	}

	for keyString, value := range d.data {
		key, err := hex.DecodeString(keyString)
		if err != nil {
			return err
		}
		_, err = fd.Write(packBytes(key, value))
		if err != nil {
			return err
		}
	}

	err = fd.Flush()
	if err != nil {
		return err
	}
	err = fd.Close()
	if err != nil {
		return err
	}

	// write checksum
	err = writeSnapshotChecksum(id, dir)
	if err != nil {
		return err
	}

	err = cleanupSnapshotsUpTo(dir, hist)
	if err != nil {
		return err
	}

	return nil
}

func load(d *db, dir string) error {
	// reset data regardless
	d.data = make(map[string][]byte)

	id, err := getMaxSnapshotId(dir)
	if err != nil {
		return err
	}

	// if id == 0 there is no snapshots to load
	if id == 0 {
		return ErrSnapshotNotFound
	}

	// verify snapshot checksum
	err = verifySnapshotChecksum(id, dir)
	if err != nil {
		return err
	}

	fd, err := getSnapshotFDForReading(id, dir)
	if err != nil {
		return err
	}

	for true {
		key, value, err := readNext(fd)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		d.data[hex.EncodeToString(key)] = value
	}

	return nil
}
