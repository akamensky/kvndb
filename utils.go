package kvndb

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func hexToBytes(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return b
}

func generateSnapshotName(n uint) string {
	return fmt.Sprintf("%06d.kvndb", n)
}

var (
	re = regexp.MustCompile(`^[0-9]{6}\.kvndb$`)
)

func isSnapshotName(s string) bool {
	return re.MatchString(s)
}

func parseSnapshotName(s string) uint {
	ds := strings.Split(s, ".")[0]
	d, err := strconv.Atoi(ds)
	if err != nil {
		panic(err)
	}
	return uint(d)
}

func getAllSnapshotIds(dir string) ([]uint, error) {
	result := make([]uint, 0)

	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, fi := range fileInfos {
		// ignore anything that is not regular file
		if !fi.Mode().IsRegular() {
			continue
		}

		// ignore any file that is not named like snapshot
		if !isSnapshotName(fi.Name()) {
			continue
		}

		result = append(result, parseSnapshotName(fi.Name()))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})

	return result, nil
}

func getLastSnapshotFDForReading(dir string) (*os.File, error) {
	maxId, err := getMaxSnapshotId(dir)
	if err != nil {
		return nil, err
	}

	if maxId == 0 {
		return nil, nil
	}

	fd, err := os.Open(getFilepath(dir, maxId))
	if err != nil {
		return nil, err
	}

	return fd, nil
}

func getNextSnapshotFDForWriting(dir string) (*os.File, error) {
	maxId, err := getMaxSnapshotId(dir)
	if err != nil {
		return nil, err
	}

	if maxId == 0 {
		maxId = 1
	}

	fd, err := os.OpenFile(getFilepath(dir, maxId), os.O_WRONLY|os.O_CREATE|os.O_SYNC, 0666)
	if err != nil {
		return nil, err
	}

	return fd, nil
}

func getFilepath(dir string, id uint) string {
	return filepath.Clean(fmt.Sprintf("%s/%s", dir, generateSnapshotName(id)))
}

func getMaxSnapshotId(dir string) (uint, error) {
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		return 0, err
	}

	var maxId uint
	for _, fi := range fileInfos {
		// ignore anything that is not regular file
		if !fi.Mode().IsRegular() {
			continue
		}

		// ignore any file that is not named like snapshot
		if !isSnapshotName(fi.Name()) {
			continue
		}

		id := parseSnapshotName(fi.Name())

		if id > maxId {
			maxId = id
		}
	}

	return maxId, nil
}

func packBytes(key, value []byte) []byte {
	result := make([]byte, 0)
	dataFrameLength := 8 + len(key) + len(value)

	result = append(result, uint32ToBytes(uint32(dataFrameLength))...)
	result = append(result, uint32ToBytes(uint32(len(key)))...)
	result = append(result, key...)
	result = append(result, uint32ToBytes(uint32(len(value)))...)
	result = append(result, value...)

	return result
}

var (
	errReadToLittle     = errors.New("io: read less than expected")
	errDataSizeMismatch = errors.New("io: data size mismatch")
)

func readNext(fd *os.File) ([]byte, []byte, error) {
	r := func(l uint32) ([]byte, error) {
		buf := make([]byte, l)
		read, err := fd.Read(buf)
		if err != nil {
			return nil, err
		}
		if read != int(l) {
			return nil, errReadToLittle
		}
		return buf, nil
	}

	dfLenBytes, err := r(4)
	if err != nil {
		return nil, nil, err
	}
	dfLen := bytesToUint32(dfLenBytes)

	kLenBytes, err := r(4)
	if err != nil {
		return nil, nil, err
	}
	kLen := bytesToUint32(kLenBytes)

	key, err := r(kLen)
	if err != nil {
		return nil, nil, err
	}

	vLenBytes, err := r(4)
	if err != nil {
		return nil, nil, err
	}
	vLen := bytesToUint32(vLenBytes)

	value, err := r(vLen)
	if err != nil {
		return nil, nil, err
	}

	if dfLen != 8+kLen+vLen {
		return nil, nil, errDataSizeMismatch
	}

	return key, value, nil
}

func bytesToUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func uint32ToBytes(data uint32) []byte {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, data)
	return bs
}

func cleanupSnapshotsUpTo(dir string, hist uint) error {
	ids, err := getAllSnapshotIds(dir)
	if err != nil {
		return err
	}

	if len(ids) <= int(hist) {
		return nil
	}

	toDelete := ids[:(len(ids) - int(hist))]

	for _, id := range toDelete {
		err = os.Remove(getFilepath(dir, id))
		if err != nil {
			return err
		}
	}

	return nil
}
