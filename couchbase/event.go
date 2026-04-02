package couchbase

import "time"

type Event struct {
	EventTime      time.Time
	Key            []byte
	Value          []byte
	CollectionName string
	Cas            uint64
	VbID           uint16
	IsDeleted      bool
	IsExpired      bool
	IsMutated      bool
}

func NewDeleteEvent(key, value []byte, collectionName string, eventTime time.Time, cas uint64, vbID uint16) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsDeleted:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
	}
}

func NewExpireEvent(key, value []byte, collectionName string, eventTime time.Time, cas uint64, vbID uint16) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsExpired:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
	}
}

func NewMutateEvent(key, value []byte, collectionName string, eventTime time.Time, cas uint64, vbID uint16) Event {
	return Event{
		Key:            key,
		Value:          value,
		IsMutated:      true,
		CollectionName: collectionName,
		EventTime:      eventTime,
		Cas:            cas,
		VbID:           vbID,
	}
}
