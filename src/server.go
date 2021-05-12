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

func setup(logger *log.Logger, connections datasources.Connections) *http.Server {
	server := newServer(connections, logWith(logger))
	return &http.Server{
		Addr:         ":8081",
		Handler:      server,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  600 * time.Second,
	}
}

func newServer(connections datasources.Connections, options ...option) *server {
	s := &server{logger: log.New(ioutil.Discard, "", 0)}

	for _, o := range options {
		o(s)
	}

	s.mux = http.NewServeMux()

	s.mux.HandleFunc("/adrese",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleAdrese(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/articole",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleArticole(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/parteneri",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleParteneri(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/vanzatori",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVanzatori(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/vanzari",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVanzari(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/liniiVanzari",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleLiniiVanzari(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/sucursale",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleSucursale(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/proiecte",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleProiecte(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/grupeArticole",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleGrupeArticole(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/um",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleUnitatiDeMasura(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/formReport",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleFormReport(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/groupedFormReport",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleGroupedFormReport(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/vanzariGrupeArticole",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVanzariGrupeArticole(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/cantitatiJudete",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleCantitatiJudete(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/discountTrimestre",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleProcentDiscountTrimestre(w, r, connections, s.logger)
		},
	)
	s.mux.HandleFunc("/volumZile",
		func(w http.ResponseWriter, r *http.Request) {
			handlers.HandleVolumMediuZile(w, r, connections, s.logger)
		},
	)

	return s
}

func main() {
	logger := log.New(os.Stdout, "", 0)
	ip := "5.12.79.189"
	connections := make(datasources.Connections, 5)
	connections[datasources.GlobalConnectionName] = datasources.GetClient("SCHEMA_PROIECT_MODB", "pass1234", fmt.Sprintf("%s:1521", ip), "MS.MSHOME.NET", datasources.GlobalConnectionName)
	connections[datasources.Local1ConnectionName] = datasources.GetClient("SCHEMA_PROIECT_MODB", "pass1234", fmt.Sprintf("%s:1522", ip), "SLV1", datasources.Local1ConnectionName)
	connections[datasources.Local2ConnectionName] = datasources.GetClient("SCHEMA_PROIECT_MODB", "pass1234", fmt.Sprintf("%s:1523", ip), "SLV2", datasources.Local2ConnectionName)
	connections[datasources.Local3ConnectionName] = datasources.GetClient("SCHEMA_PROIECT_MODB", "pass1234", fmt.Sprintf("%s:1524", ip), "SLV3", datasources.Local3ConnectionName)
	connections[datasources.Local4ConnectionName] = datasources.GetClient("SCHEMA_PROIECT_MODB", "pass1234", fmt.Sprintf("%s:1525", ip), "SLV4", datasources.Local4ConnectionName)
	hs := setup(logger, connections)

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
