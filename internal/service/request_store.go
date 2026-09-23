package service

import sink "github.com/batchstream/sink-protocol/sink/v1"

type addressedOperation interface {
	GetAddress() *sink.RecordAddress
}
