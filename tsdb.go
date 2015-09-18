package tsdb

// DataSource .
type DataSource interface {
	// Open open ts database
	Open(name string)
	// Update update key's value or create new one
	Update(key string, data []byte) error
	// After get key's version values
	Query(key string, version uint64) (DataSet, error)
	// LastVersion get value's last version
	LastVersion(key string) (version uint64, err error)
	// Close close data source
	Close()
}

// DataSet .
type DataSet interface {
	Next() (data []byte, version uint64)
	Close()
}
