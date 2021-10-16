package kvndb

import (
	"bytes"
	"encoding/hex"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestKvndbSaveLoad(t *testing.T) {
	// generate test data 1 mln random bytes sequences
	testData := make(map[string][]byte)
	rand.Seed(time.Now().Unix())
	dKey := make([]byte, 8)
	dVal := make([]byte, 8)
	var err error
	for i := 0; i < 1_000_000; i++ {
		_, err = rand.Read(dKey)
		if err != nil {
			t.Fatal(err)
		}
		_, err = rand.Read(dVal)
		if err != nil {
			t.Fatal(err)
		}
		testData[hex.EncodeToString(dKey)] = dVal
	}

	// make new tmp dir for data
	dir, err := os.MkdirTemp(".", "temp-*")
	if err != nil && err.Error() != "mkdir ./temp: file exists" {
		t.Fatal(err)
	}

	testKvndbSave(t, dir, testData)
	loadedData := testKvndbLoad(t, dir)
	if len(loadedData) != len(testData) {
		t.Fatalf("loaded data size mismatch; test data size [%d], but loaded [%d]", len(testData), len(loadedData))
	}
	for k, tv := range testData {
		if v, ok := loadedData[k]; ok {
			if !bytes.Equal(tv, v) {
				t.Fatalf("slices are not equal. expected [%s], but got [%s]", hex.EncodeToString(tv), hex.EncodeToString(v))
			}
		} else {
			t.Fatalf("loaded data missing key [%s]", k)
		}
	}
}

func testKvndbSave(t *testing.T, dir string, testData map[string][]byte) {
	d := New()

	for k, v := range testData {
		d.Put(hexToBytes(k), v)
	}

	err := d.Save(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
}

func testKvndbLoad(t *testing.T, dir string) map[string][]byte {
	d := newDb()

	err := d.Load(dir)
	if err != nil {
		t.Fatal(err)
	}

	return d.data
}
