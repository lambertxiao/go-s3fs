//go:build !windows
// +build !windows

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/common"
	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/fs"
	"github.com/lambertxiao/go-s3fs/pkg/logg"
	"github.com/lambertxiao/go-s3fs/pkg/types"

	_ "net/http/pprof"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	gd "github.com/sevlyar/go-daemon"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

func mount(bucket string, fsconfig *config.FSConfig, c *cli.Context) error {
	notify_process := func(pid int, si os.Signal) {
		p, err := os.FindProcess(pid)
		if err != nil {
			logg.Dlog.Errorf("notify_process %v, %v", pid, err)
			return
		}
		defer p.Release()
		err = p.Signal(si)
		if err != nil {
			logg.Dlog.Errorf("notify_process %v, %v", pid, err)
			return
		}
	}

	if !fsconfig.Foreground {
		ctx := new(gd.Context)
		d, err := ctx.Reborn()
		if err != nil {
			fmt.Println("error to fork child process", err)
			return err
		}

		var waitfor sync.WaitGroup
		waitfor_child := func() {
			sigs := make(chan os.Signal, 1)
			signal.Notify(sigs, syscall.SIGUSR1, syscall.SIGUSR2)
			waitfor.Add(1)
			go func() {
				waitfor_sig = <-sigs
				waitfor.Done()
			}()
		}
		waitfor_child()

		if d != nil {
			// parent process
			waitfor.Wait()

			if waitfor_sig == syscall.SIGUSR1 {
				return nil
			} else {
				fmt.Println("Mount Error")
				return types.EINVAL
			}
		} else {
			// child process
			// kill child's waitfor_sig
			notify_process(os.Getpid(), syscall.SIGUSR1)
			waitfor.Wait()
			defer ctx.Release()
		}
	}

	mfs, err := Mount(fsconfig, bucket, c)
	if err != nil {
		if !fsconfig.Foreground {
			notify_process(os.Getppid(), syscall.SIGUSR2)
		}
		logg.Dlog.Fatalf("mount error, %v", err)
	} else {
		logg.Dlog.Infof("succ mount")

		if !fsconfig.Foreground {
			notify_process(os.Getppid(), syscall.SIGUSR1)
		}
		RegisterSignalHandler()

		go func() {
			port := 8899
			for {
				err := http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", port), nil)
				if err == nil {
					break
				}
				logg.Dlog.Error(err)
				port--
			}
		}()

		err = mfs.Join(context.Background())
		if err != nil {
			logg.Dlog.Fatalf("umount error, %v", err)
		}

		logg.Dlog.Println("succ exit")
	}

	return err
}

func Mount(conf *config.FSConfig, bucket string, c *cli.Context) (*fuse.MountedFileSystem, error) {
	logg.Dlog.Infof("GO_S3FS_VERSION:%s, COMMIT_ID:%s, GO_VERSION:%s, BUILD_TIME:%s",
		types.GO_S3FS_VERSION, types.COMMIT_ID, types.GO_VERSION, types.BUILD_TIME)
	logg.Dlog.Infof("startup params: %+v", *conf)

	if !conf.Foreground {
		logDir := conf.LogDir
		if logDir == "" {
			homedir := os.Getenv("HOME")
			if homedir == "" {
				log.Panicf("HOME environment variable is empty")
			}
			logDir = homedir + "/.go-s3fs"
		}

		err := fs.RedirectStderr(logDir, types.PANIC_LOG_PREFIX, types.PANIC_LOG_SUFFIX)
		if err != nil {
			return nil, err
		}
	}

	mountConf := &fuse.MountConfig{
		FSName:                    "go-s3fs",
		DisableWritebackCaching:   !conf.Writeback,
		EnableAsyncReads:          !conf.Disable_async_read,
		ErrorLogger:               log.New(logg.Dfuseerrlog.WriterLevel(logrus.ErrorLevel), "", 0),
		Options:                   conf.FuseOptions,
		DisableDefaultPermissions: !conf.Allow_other,
	}

	if conf.DebugFuse {
		logg.Dfuselog.Level = logrus.DebugLevel
		mountConf.DebugLogger = log.New(logg.Dfuselog.WriterLevel(logrus.DebugLevel), "", 0)
	}

	registry, registerer := fs.InitMetricRegistry(conf.MountPoint, bucket)
	fs.RegistMetrics(registerer)

	fsx, err := fs.InitFS(conf, bucket, registry, registerer,
		func(fs *fs.FSR) {
			reloadCfg(fs, c)
		},
	)

	if err != nil {
		return nil, err
	}

	server := fuseutil.NewFileSystemServer(&fs.FSRX{FSR: fsx})
	mfs, err := fuse.Mount(conf.MountPoint, server, mountConf)

	return mfs, err
}

func tryUnmount() error {
	var err error = nil
	for i := 0; i < 10; i++ {
		err = fuse.Unmount(config.GetGConfig().MountPoint)
		if err != nil {
			time.Sleep(time.Second)
		} else {
			return nil
		}
	}
	return err
}

func RegisterSignalHandler() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	h := func() {
		for {
			s := <-sigs
			logg.Dlog.Infof("try to umount")
			err := tryUnmount()
			if err != nil {
				logg.Dlog.Errorf("umount error: %v, %v", err, s)
			} else {
				logg.Dlog.Infof("umount succ %v", s)
			}
			return
		}
	}

	go h()
}

func reloadCfg(fsx *fs.FSR, c *cli.Context) {
	confPath := config.GetGConfig().Passwd_file
	fi, err := os.Stat(confPath)
	if err != nil {
		logg.Dlog.Error(err)
		return
	}

	if fi.ModTime() != config.GetGConfig().ModifiedTime {
		logg.Dlog.Infof("cfg %s is modified, mod_time:%d, reload it", confPath, fi.ModTime().Unix())

		// 重新根据命令行参数和配置文件生成一个config
		newCfg, err := PopulateConfig(c)
		if err != nil {
			logg.Dlog.Error(err)
			return
		}

		config.SetGConfig(newCfg)
		logg.Dlog.SetLevel(newCfg.Log_level)
		logg.Dlog.Infof("cfg after modified %+v", newCfg)

		fsx.ReloadFS(newCfg)

		// 并发池需要重新生成
		common.InitParallelPool(newCfg.Parallel)
	}
}
