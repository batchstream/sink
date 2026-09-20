package service

import "github.com/batchstream/sink/internal/storage"

// Record identity is the complete canonical URI, independent of the backend.
type recordIdentity string

func identityOf(address storage.Address) recordIdentity { return recordIdentity(address.String()) }
