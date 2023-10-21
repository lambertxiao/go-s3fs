package types

import "io"

type NullBuf struct{}

func (b NullBuf) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (b NullBuf) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

var DefaultNullBuf = NullBuf{}
