package kvndb

import (
	"encoding/hex"
	"io"
)

func save(d *db, dir string, hist uint) error {
	fd, err := getNextSnapshotFDForWriting(dir)
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

	err = fd.Sync()
	if err != nil {
		return err
	}
	err = fd.Close()
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

	fd, err := getLastSnapshotFDForReading(dir)
	if err != nil {
		return err
	}
	if fd == nil {
		return ErrSnapshotNotFound
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

	err = fd.Close()
	if err != nil {
		return err
	}

	return nil
}
