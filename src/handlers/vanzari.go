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

func HandleVanzari(w http.ResponseWriter, r *http.Request, connections datasources.Connections, logger *log.Logger) {
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
		response, status, err = getVanzari(db, logger)
	case http.MethodPost:
		status, err = insertVanzare(r, db, connections[datasources.GlobalConnectionName], logger)
	default:
		status = http.StatusBadRequest
		err = errors.New("wrong method type for /vanzari route")
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

func getVanzari(db datasources.DBClient, logger *log.Logger) ([]byte, int, error) {
	vanzari, err := db.GetVanzari()
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return nil, http.StatusInternalServerError, errors.New("could not get vanzari")
	}

	response, err := json.Marshal(vanzari)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("could not marshal vanzari response json")
	}

	return response, http.StatusOK, nil
}

func extractVanzareParams(r *http.Request) (repositories.InsertVanzare, error) {
	var unmarshalledvanzare repositories.InsertVanzare

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return repositories.InsertVanzare{}, err
	}

	err = json.Unmarshal(body, &unmarshalledvanzare)
	if err != nil {
		return repositories.InsertVanzare{}, err
	}

	return unmarshalledvanzare, nil
}

func insertVanzare(r *http.Request, db datasources.DBClient, general datasources.DBClient, logger *log.Logger) (int, error) {
	vanzare, err := extractVanzareParams(r)
	if err != nil {
		return http.StatusBadRequest, errors.New("vanzare information sent on request body does not match required format")
	}

	err = db.InsertVanzare(vanzare, general)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return http.StatusInternalServerError, errors.New("could not save vanzare")
	}

	return http.StatusOK, nil
}
