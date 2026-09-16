package service

import sink "github.com/liran/sink/gen/sink"

type addressedOperation interface {
	GetAddress() *sink.RecordAddress
}
