package store

import (
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/dzdx/raft/raftpb"
	"github.com/dzdx/raft/util"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
)

var (
	logger *logrus.Logger
)

var (
	keyLogEntry = []byte("logEntry")
	keyKV       = []byte("kv")
)

type BoltdbStore struct {
	db       *bolt.DB
	maxIndex uint64
}

func (store *BoltdbStore) GetEntries(start, end uint64) ([]*raftpb.LogEntry, error) {
	result := make([]*raftpb.LogEntry, end-start+1)
	err := store.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(keyLogEntry)
		for index := start; index <= end; index++ {
			vb := b.Get(util.Uint64ToBytes(index))
			if vb == nil {
				return NewErrNotFound("logEntry %d not found", index)
			}
			entry := &raftpb.LogEntry{}
			if err := proto.Unmarshal(vb, entry); err != nil {
				return err
			}
			result[index-start] = entry
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (store *BoltdbStore) GetEntry(index uint64) (*raftpb.LogEntry, error) {

	es, err := store.GetEntries(index, index)
	if err != nil {
		return nil, err
	}
	return es[0], nil
}

func (store *BoltdbStore) AppendEntry(entry *raftpb.LogEntry) error {
	return store.AppendEntries([]*raftpb.LogEntry{entry})
}

func (store *BoltdbStore) AppendEntries(entries []*raftpb.LogEntry) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(keyLogEntry)
		for _, entry := range entries {
			key := util.Uint64ToBytes(entry.Index)
			vb, _ := proto.Marshal(entry)
			if err := b.Put(key, vb); err != nil {
				return err
			}
		}
		return nil
	})
}

func (store *BoltdbStore) DeleteEntries(min uint64, max uint64) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(keyLogEntry)
		for key := min; key <= max; key++ {
			b.Delete(util.Uint64ToBytes(key))
		}
		return nil
	})
}
func (store *BoltdbStore) FirstIndex() (uint64, error) {
	var lastIndex uint64
	err := store.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket(keyLogEntry).Cursor()
		var first []byte
		if first, _ = cur.Next(); first == nil {
			return nil
		}
		lastIndex = util.BytesToUint64(first)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return lastIndex, nil
}
func (store *BoltdbStore) LastIndex() (uint64, error) {
	var lastIndex uint64
	err := store.db.View(func(tx *bolt.Tx) error {
		cur := tx.Bucket(keyLogEntry).Cursor()
		var last []byte
		if last, _ = cur.Last(); last == nil {
			return nil
		}
		lastIndex = util.BytesToUint64(last)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return lastIndex, nil
}

func (store BoltdbStore) SetKV(key string, value []byte) error {
	return store.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(keyKV)
		err := b.Put([]byte(key), value)
		return err
	})
}

func (store BoltdbStore) GetKV(key string) ([]byte, error) {
	var vb []byte
	err := store.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(keyKV)
		vb = b.Get([]byte(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if vb == nil {
		return nil, NewErrNotFound("key %s not found", key)
	}
	return vb, nil
}

func newBoltdb(path string) *bolt.DB {
	var db *bolt.DB
	var err error

	if db, err = bolt.Open(path, 0644, nil); err != nil {
		logger.Fatalf("open boltdb failed: %v", err)
	}
	if err != nil {
		logger.Fatal(err)
	}
	return db
}

func NewBoltdbStore(path string) *BoltdbStore {
	db := newBoltdb(path)
	err := db.Update(func(tx *bolt.Tx) error {
		var err error
		if _, err = tx.CreateBucketIfNotExists(keyLogEntry); err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		if err != nil {
		}
		if _, err = tx.CreateBucketIfNotExists(keyKV); err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	if err != nil {
		logger.Fatal(err)
	}
	store := &BoltdbStore{db: db}
	return store
}
