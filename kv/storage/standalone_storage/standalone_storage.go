package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db     *badger.DB
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{db: nil, config: conf}
}

func (s *StandAloneStorage) Start() error {
	os.MkdirAll(s.config.DBPath, os.ModePerm)
	s.db = engine_util.CreateDB(s.config.DBPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.db.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return nil, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, modify := range batch {
		switch modify.Data.(type) {
		case storage.Put:
			{
				putOp := modify.Data.(storage.Put)
				err := engine_util.PutCF(s.db, putOp.Cf, putOp.Key, putOp.Value)
				if err != nil {
					return err
				}
			}
		case storage.Delete:
			{
				deleteOp := modify.Data.(storage.Delete)
				err := engine_util.DeleteCF(s.db, deleteOp.Cf, deleteOp.Key)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
