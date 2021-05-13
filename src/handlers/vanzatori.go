package handlers

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"

	"modbSalesApp/src/datasources"
	"modbSalesApp/src/repositories"
)

func HandleVanzatori(w http.ResponseWriter, r *http.Request, connections datasources.Connections, logger *log.Logger) {
	var response []byte
	var status int
	var err error

	db := getDatabase(r, connections)

	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-CSRF-Token")
		w.Header().Set("Access-Control-Expose-Headers", "Authorization")
	case http.MethodGet:
		response, status, err = getVanzatori(db, logger)
	case http.MethodPost:
		status, err = insertVanzator(r, connections[datasources.GlobalConnectionName], logger)
	default:
		status = http.StatusBadRequest
		err = errors.New("wrong method type for /vanzatori route")
	}

	if err != nil {
		logger.Printf("Error: %s; Status: %d %s", err.Error(), status, http.StatusText(status))
		http.Error(w, err.Error(), status)

		return
	}

	if response == nil {
		response, _ = json.Marshal(repositories.WasSuccess{Success: true})
	}

	_, err = w.Write(response)
	if err != nil {
		status = http.StatusInternalServerError
		logger.Printf("Error: %s; Status: %d %s", err.Error(), status, http.StatusText(status))
		http.Error(w, err.Error(), status)

		return
	}

	status = http.StatusOK
	logger.Printf("Status: %d %s", status, http.StatusText(status))
}

func getVanzatori(db datasources.DBClient, logger *log.Logger) ([]byte, int, error) {
	vanzatori, err := db.GetVanzatori()
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return nil, http.StatusInternalServerError, errors.New("could not get vanzatori")
	}

	response, err := json.Marshal(vanzatori)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("could not marshal vanzatori response json")
	}

	return response, http.StatusOK, nil
}

func extractVanzatorParams(r *http.Request) (repositories.InsertVanzator, error) {
	var unmarshalledVanzator repositories.InsertVanzator

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return repositories.InsertVanzator{}, err
	}

	err = json.Unmarshal(body, &unmarshalledVanzator)
	if err != nil {
		return repositories.InsertVanzator{}, err
	}

	return unmarshalledVanzator, nil
}

func insertVanzator(r *http.Request, db datasources.DBClient, logger *log.Logger) (int, error) {
	vanzator, err := extractVanzatorParams(r)
	if err != nil {
		return http.StatusBadRequest, errors.New("vanzator information sent on request body does not match required format")
	}

	err = db.InsertVanzator(vanzator)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return http.StatusInternalServerError, errors.New("could not save vanzator")
	}

	return http.StatusOK, nil
}
