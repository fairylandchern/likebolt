package likebolt

import "errors"

var (
	ErrInvalid         = errors.New("invalid database")
	ErrChecksum        = errors.New("checksum error")
	ErrVersionMismatch = errors.New("version mismatch")

	ErrTxClosed = errors.New("tx closed")

	// ErrKeyRequired is returned when inserting a zero-length key.
	ErrKeyRequired = errors.New("key required")

	// ErrKeyTooLarge is returned when inserting a key that is larger than MaxKeySize.
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge is returned when inserting a value that is larger than MaxValueSize.
	ErrValueTooLarge = errors.New("value too large")

	// ErrIncompatibleValue is returned when trying create or delete a bucket
	// on an existing non-bucket key or when trying to create or delete a
	// non-bucket key on an existing bucket key.
	ErrIncompatibleValue = errors.New("incompatible value")
)
