package fs

import (
	"context"

	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/types"
)

func (fs *FSR) removeXattr(ctx context.Context) error {
	logg.Dlog.Warnf("remove xattr not support ")
	return types.ENOSYS
}

func (fs *FSR) getXattr(
	ctx context.Context) error {
	return nil
}

func (fs *FSR) listXattr(ctx context.Context) error {
	logg.Dlog.Warnf("lsxattr not support")
	return types.ENOSYS
}

func (fs *FSR) setXattr(ctx context.Context) error {
	logg.Dlog.Warnf("setxattr not support")
	return types.ENOSYS
}
