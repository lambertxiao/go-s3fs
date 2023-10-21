package fwriter

type FileWriter interface {
	Write(offset int64, data []byte) (int, error)
	Flush() error
	Release() error
	FileSize() uint64
}
