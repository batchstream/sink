package service

import sink "github.com/batchstream/sink/gen/sink"

type addressedOperation interface {
	GetAddress() *sink.RecordAddress
}
