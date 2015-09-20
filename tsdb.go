package tsdb

// DataSource .
type DataSource interface {
	// Update update key's value or create new one
	Update(key string, data []byte) error
	// Query get key's version values
	Query(key string, version uint64) (DataSet, error)
	// Close close data source
	Close()
}

// DataSet .
type DataSet interface {
	MiniVersion() uint64
	Next() (data []byte, version uint64)
	Close()
}

// SEQIDGen .
type SEQIDGen interface {
	Close()
	SQID(key string) (uint64, error)
}

// Persistence .
type Persistence interface {
	Close()
	Storage(key string) (Storage, error)
}

// Storage .
type Storage interface {
	Close()
	Write(val *DBValue) error
	Read(version uint64) (*DBValue, bool)
}
