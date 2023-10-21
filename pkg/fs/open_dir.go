package fs

import "github.com/lambertxiao/go-s3fs/pkg/types"

type OpenDir struct {
	ino     types.InodeID
	hid     HandleID
	dirents []types.Dirent
}
