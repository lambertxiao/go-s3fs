package freader

type FileReader interface {
	Read(offset uint64, length int, data []byte) (int, error)
	Release() error
}
