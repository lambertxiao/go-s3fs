package types

import "errors"

var (
	EEXIST    error = errors.New("file exists")
	EINVAL    error = errors.New("invalid argument")
	EIO       error = errors.New("input/output error")
	ENOATTR   error = errors.New("no data available")
	ENOENT    error = errors.New("no such file or directory")
	ENOSYS    error = errors.New("function not implemented")
	ENOTDIR   error = errors.New("not a directory")
	EISDIR    error = errors.New("is a directory")
	ENOTEMPTY error = errors.New("directory not empty")
	EPERM     error = errors.New("Operation not permitted")
)

var (
	ErrPartSize = errors.New("part size err")
)
