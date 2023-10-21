package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/lambertxiao/go-s3fs/pkg/config"
	"github.com/lambertxiao/go-s3fs/pkg/logg"

	"github.com/urfave/cli"

	_ "net/http/pprof"
)

var waitfor_sig os.Signal

func init() {
	os.Setenv("GOTRACEBACK", "crash")

	go func() {
		for {
			time.Sleep(time.Minute * 10)
			runtime.GC()
		}
	}()
}

func main() {
	app := NewApp()

	app.Action = func(c *cli.Context) error {
		if len(c.Args()) < 2 {
			fmt.Fprintf(os.Stderr, "Error: %s need two arg.\n", app.Name)
			cli.ShowAppHelp(c)
			os.Exit(1)
		}

		fsconfig, err := PopulateConfig(c)
		if err != nil {
			fmt.Printf("Parse config error: %v\n", err)
			return err
		}

		config.SetGConfig(fsconfig)
		logg.InitLogHook(fsconfig.LogDir, fsconfig.LogMaxAge, fsconfig.LogRotationTime)
		logg.InitLogger()

		bucket := c.Args()[0]
		return mount(bucket, fsconfig, c)
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println("See log for defail reason", err)
		os.Exit(1)
	}
}
