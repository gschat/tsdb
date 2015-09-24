package tsdb

// DataSource .
type DataSource interface {
	// Update update key's value or create new one
	Update(key string, data []byte) error
	// Query get key's version values
	Query(key string, version uint64) (DataSet, error)
	// Close close data source
	Close()
	// get value version of key
	CurrentVersion(key string) (uint64, bool)
}

// DataSet .
type DataSet interface {
	MiniVersion() uint64
	Stream() <-chan *DBValue
	Close()
}

// SEQIDGen .
type SEQIDGen interface {
	Close()
	Current(key string) (uint64, bool)
	SQID(key string) (uint64, error)
}

// Storage .
type Storage interface {
	Close()
	Write(key string, val *DBValue) error
	Read(key string, version uint64) (*DBValue, bool)
}
