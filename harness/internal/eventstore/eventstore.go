package eventstore

import (
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type Store struct {
	store *store.Store
}

type Query = store.EventEnvelopeQuery

type Record = store.EventEnvelopeRecord

func Open(path string) (*Store, error) {
	st, err := store.OpenStore(path)
	if err != nil {
		return nil, err
	}
	return &Store{store: st}, nil
}

func (s *Store) Close() error {
	return s.store.Close()
}

func (s *Store) Append(env eventmodel.EventEnvelope) (int64, error) {
	return s.store.AppendEventEnvelope(env)
}

func (s *Store) Read(q Query) ([]Record, error) {
	return s.store.EventEnvelopes(q)
}
