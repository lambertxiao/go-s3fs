package main

import (
	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/types"
	"github.com/urfave/cli"
)

func FillConfig(c *cli.Context, conf *config.FSConfig) {
	conf.Entry_ttl = c.Duration(C_ENTRYTIMEOUT)
	conf.Attr_ttl = c.Duration(C_ATTRTIMEOUT)
	conf.Disable_async_read = c.Bool(C_DISABLE_ASYNC_READ)
	conf.Max_background = uint16(c.Int(C_MAX_BACKGROUND))
	conf.Congestion_threshold = uint16(c.Int(C_CONGESTION_THRESHOLD))
	conf.Async_dio = c.Bool(C_ASYNC_DIO)
	conf.Keep_pagecache = c.Bool(C_KEEP_PAGECACHE)
}

func AppendAppFlags(app *cli.App) {
	app.Flags = append(app.Flags, []cli.Flag{
		cli.DurationFlag{
			Name:  C_ENTRYTIMEOUT,
			Value: types.DEFAULT_ENTRY_TTL,
			Usage: "How long to cache dentry for inode for fuse.",
		}, cli.DurationFlag{
			Name:  C_ATTRTIMEOUT,
			Value: types.DEFAULT_ATTR_TTL,
			Usage: "How long to cache inode attr for fuse",
		},
		cli.BoolFlag{
			Name:  C_DISABLE_ASYNC_READ,
			Usage: "Disable all read (even read-ahead) operations asynchronously",
		},
		cli.IntFlag{
			Name:  C_MAX_BACKGROUND,
			Value: types.DEFAULT_BACKGROUND,
			Usage: "Specify the max_background parameter of fuse kernel(>=7.13), currently fuse usespace supports up to 1024",
		},
		cli.IntFlag{
			Name:  C_CONGESTION_THRESHOLD,
			Value: types.DEFAULT_CONGESTION_THRESHOLD,
			Usage: "Specify the congestion_threshold parameter of fuse kernel(>=7.13), currently fuse usespace supports up to 768",
		},
		cli.BoolFlag{
			Name:  C_ASYNC_DIO,
			Usage: "Enable the async_dio parameter of fuse kernel, async_dio is disabled by default",
		},
		cli.BoolFlag{
			Name:  C_KEEP_PAGECACHE,
			Usage: "Turn on pagecache, when the file is opened, it will be decided whether to update according to the modification time of the inode, so please pay attention to the attr_timeout and dcache_timeout parameters will have a certain impact on this",
		}}...)
}

func FillPlatformFlags(allFlags map[string]string) {
	for _, v := range []string{
		C_ENTRYTIMEOUT,
		C_ATTRTIMEOUT,
		C_DCACHETIMEOUT,
		C_DISABLE_ASYNC_READ,
		C_MAX_BACKGROUND,
		C_CONGESTION_THRESHOLD,
		C_ASYNC_DIO,
		C_KEEP_PAGECACHE,
		C_O} {
		allFlags[v] = "fuse"
	}
}

func PlatformAppHelpTemplate() string {
	return `
FUSE
	{{range cate .Flags "fuse"}}{{.}}
	{{end}}
`
}
