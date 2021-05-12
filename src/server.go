package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"modbSalesApp/src/datasources"
	"modbSalesApp/src/handlers"
)

type server struct {
	mux    *http.ServeMux
	logger *log.Logger
}

type option func(*server)

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.log("Method: %s, Path: %s", r.Method, r.URL.Path)
	s.mux.ServeHTTP(w, r)
}

func (s *server) log(format string, v ...interface{}) {
	s.logger.Printf(format+"\n", v...)
}

func logWith(logger *log.Logger) option {
	return func(s *server) {
		s.logger = logger
	}
}

func setup(logger *log.Logger, db datasources.DBClient, dw datasources.DBClient) *http.Server {
	server := newServer(db, dw, logWith(logger))
	return &http.Server{
		Addr:         ":8081",
		Handler:      server,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  600 * time.Second,
	}
}

func newServer(db datasources.DBClient, dw datasources.DBClient, options ...option) *server {
	s := &server{logger: log.New(ioutil.Discard, "", 0)}

	for _, o := range options {
		o(s)
	}

	s.mux = http.NewServeMux()

	s.mux.HandleFunc("/articole",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleArticole(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/parteneri",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleParteneri(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/vanzatori",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVanzatori(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/vanzari",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVanzari(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/liniiVanzari",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleLiniiVanzari(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/sucursale",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleSucursale(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/proiecte",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleProiecte(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/grupeArticole",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleGrupeArticole(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/um",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleUnitatiDeMasura(w, r, db, s.logger)
		},
	)
	s.mux.HandleFunc("/formReport",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleFormReport(w, r, dw, s.logger)
		},
	)
	s.mux.HandleFunc("/groupedFormReport",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleGroupedFormReport(w, r, dw, s.logger)
		},
	)
	s.mux.HandleFunc("/vanzariGrupeArticole",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVanzariGrupeArticole(w, r, dw, s.logger)
		},
	)
	s.mux.HandleFunc("/cantitatiJudete",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleCantitatiJudete(w, r, dw, s.logger)
		},
	)
	s.mux.HandleFunc("/discountTrimestre",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleProcentDiscountTrimestre(w, r, dw, s.logger)
		},
	)
	s.mux.HandleFunc("/volumZile",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVolumMediuZile(w, r, dw, s.logger)
		},
	)

	return s
}

func main() {
	logger := log.New(os.Stdout, "", 0)
	ip := "86.127.133.101"
	db := datasources.GetClient("SCHEMA_PROIECT_OLDB", "pass1234", fmt.Sprintf("%s:1521", ip), "ORCL.DOCKER.INTERNAL")
	dw := datasources.GetClient("SCHEMA_PROIECT_OLAP", "pass1234", fmt.Sprintf("%s:1521", ip), "ORCL.DOCKER.INTERNAL")
	hs := setup(logger, db, dw)

	logger.Printf("Listening on http://localhost%s\n", hs.Addr)
	go func() {
		if err := hs.ListenAndServe(); err != nil {
			logger.Println(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	<-signals

	logger.Println("Shutting down webserver.")
	os.Exit(0)
}
