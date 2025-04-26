package main

import (
	"context"
	"crypto/tls"
	"db/internal/app"
	"db/internal/engine"
	"db/internal/server"
)

func main() {
	application, err := initialize()
	if err != nil {
		panic(err)
	}

	if err = application.Run(context.Background()); err != nil {
		panic(err)
	}
}

func initialize() (*app.App, error) {
	var deps []app.Dependency

	// load the TLS certificate and key, ideally this is configuration based on deployements, but
	// for now we can roll with it.
	// TODO: make certificate requirements configurable
	cert, err := tls.LoadX509KeyPair("local/local_cert.pem", "local/local_key.pem")
	if err != nil {
		return nil, err
	}

	// create the litetable engine
	engineHandler, err := engine.New()
	if err != nil {
		return nil, err
	}

	// create a LiteTable server
	srv, err := server.New(&server.Config{
		Certificate: &cert,
		Port:        "9443",
		Handler:     engineHandler,
	})
	if err != nil {
		return nil, err
	}

	deps = append(deps, srv)

	application, err := app.CreateApp(&app.Config{
		ServiceName: "LiteTable DB",
		StopTimeout: 5,
	}, deps...)
	if err != nil {
		return nil, err
	}

	return application, nil
}
