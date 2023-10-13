package main

import (
	"context"
	"errors"
	"github.com/mskcc/smile/internal/dremio"
	"github.com/mskcc/smile/internal/smile"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
)

func setupOptions() {
	pflag.StringP("cfg_file", "f", "", "Path to configuration file")
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
}

func parseArgs() (dremio.DremioArgs, smile.SmileArgs, error) {
	if viper.GetBool("help") {
		pflag.PrintDefaults()
		os.Exit(0)
	}
	var DremioArgs dremio.DremioArgs
	var SmileArgs smile.SmileArgs
	cf := viper.GetString("cfg_file")
	if cf == "" {
		return DremioArgs, SmileArgs, errors.New("Missing cfg_file argument")
	}
	viper.SetConfigName(filepath.Base(cf))
	viper.SetConfigType(strings.TrimPrefix(filepath.Ext(cf), "."))
	viper.AddConfigPath(filepath.Dir(cf))
	if err := viper.ReadInConfig(); err != nil {
		return DremioArgs, SmileArgs, errors.New("Cannot read cfg_file")
	}
	if DremioArgs.Host = viper.GetString("dremio.host"); DremioArgs.Host == "" {
		return DremioArgs, SmileArgs, errors.New("Missing dremio.host property in config file")
	}
	if DremioArgs.Username = viper.GetString("dremio.username"); DremioArgs.Username == "" {
		return DremioArgs, SmileArgs, errors.New("Missing dremio.username property in config file")
	}
	if DremioArgs.Password = viper.GetString("dremio.password"); DremioArgs.Password == "" {
		return DremioArgs, SmileArgs, errors.New("Missing dremio.password property in config file")
	}
	if DremioArgs.ObjectStore = viper.GetString("dremio.objectstore"); DremioArgs.ObjectStore == "" {
		return DremioArgs, SmileArgs, errors.New("Missing dremio.objectstore property in config file")
	}
	if DremioArgs.RequestTable = viper.GetString("dremio.requesttable"); DremioArgs.RequestTable == "" {
		return DremioArgs, SmileArgs, errors.New("Missing dremio.requesttable property in config file")
	}
	if DremioArgs.SampleTable = viper.GetString("dremio.sampletable"); DremioArgs.SampleTable == "" {
		return DremioArgs, SmileArgs, errors.New("Missing dremio.sampletable property in config file")
	}

	if SmileArgs.URL = viper.GetString("smile.url"); SmileArgs.URL == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.url property in config file")
	}
	if SmileArgs.CertPath = viper.GetString("smile.certpath"); SmileArgs.CertPath == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.certpath property in config file")
	}
	SmileArgs.CertPath = os.ExpandEnv(SmileArgs.CertPath)
	if SmileArgs.KeyPath = viper.GetString("smile.keypath"); SmileArgs.KeyPath == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.keypath property in config file")
	}
	SmileArgs.KeyPath = os.ExpandEnv(SmileArgs.KeyPath)
	if SmileArgs.Consumer = viper.GetString("smile.consumer"); SmileArgs.Consumer == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.consumer property in config file")
	}
	if SmileArgs.Password = viper.GetString("smile.password"); SmileArgs.Password == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.password property in config file")
	}
	if SmileArgs.Subject = viper.GetString("smile.subject"); SmileArgs.Subject == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.subject property in config file")
	}
	if SmileArgs.NewRequestFilter = viper.GetString("smile.newrequestfilter"); SmileArgs.NewRequestFilter == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.newrequestfilter property in config file")
	}
	if SmileArgs.UpdateRequestFilter = viper.GetString("smile.updaterequestfilter"); SmileArgs.UpdateRequestFilter == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.updaterequestfilter property in config file")
	}
	if SmileArgs.UpdateSampleFilter = viper.GetString("smile.updatesamplefilter"); SmileArgs.UpdateSampleFilter == "" {
		return DremioArgs, SmileArgs, errors.New("Missing smile.updatesamplefilter property in config file")
	}

	return DremioArgs, SmileArgs, nil
}

func setupSignalListener(cancel context.CancelFunc) {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func() {
		// block until signal is received
		s := <-c
		log.Printf("Got signal: %s, canceling context...\n", s)
		cancel()
	}()
}

func main() {
	setupOptions()
	dArgs, sArgs, err := parseArgs()
	if err != nil {
		log.Fatal("failed to parse arguments: ", err)
	}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	setupSignalListener(cancel)

	dRepo, err := dremio.NewDremioRepos(dArgs)
	if err != nil {
		log.Fatal("failed to create repos: ", err)
	}

	svc, err := smile.NewService(sArgs, dRepo)
	if err != nil {
		log.Fatal("failed to create a service: ", err)
	}

	if err := svc.Run(ctx); err != nil {
		os.Exit(1)
	}
	log.Println("Exiting...")
}
