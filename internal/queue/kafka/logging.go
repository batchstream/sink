package kafka

import (
	"log/slog"
	"strconv"

	sink "github.com/liran/sink/gen/sink"
	"github.com/liran/sink/internal/logging"
	"github.com/liran/sink/internal/queue"
)

type quarantinedDocument struct{ envelope []byte }

func (d quarantinedDocument) LogValue() slog.Value {
	return slog.AnyValue(quarantinedBody(d.envelope))
}

func quarantinedBody(envelope []byte) *logging.FailureBody {
	mutation, err := queue.UnmarshalMutation(envelope)
	if err != nil || mutation.Write == nil {
		return nil
	}
	document := mutation.Write.GetPut().GetDocument()
	if document == nil {
		document = mutation.Write.GetMerge().GetIncomingDocument()
	}
	if document == nil {
		return nil
	}
	encoding := "enum:" + strconv.FormatInt(int64(document.GetEncoding()), 10)
	switch document.GetEncoding() {
	case sink.DocumentEncoding_DOCUMENT_ENCODING_BSON:
		encoding = "bson"
	case sink.DocumentEncoding_DOCUMENT_ENCODING_JSON:
		encoding = "json"
	}
	body := &logging.FailureBody{Encoding: encoding, Payload: document.GetPayload()}
	return body
}
