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

func HandleSucursale(w http.ResponseWriter, r *http.Request, connections datasources.Connections, logger *log.Logger) {
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
		response, status, err = getSucursale(db, logger)
	case http.MethodPost:
		status, err = insertSucursala(r, db, logger)
	default:
		status = http.StatusBadRequest
		err = errors.New("wrong method type for /sucursale route")
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

func getSucursale(db datasources.DBClient, logger *log.Logger) ([]byte, int, error) {
	sucursale, err := db.GetSucursale()
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return nil, http.StatusInternalServerError, errors.New("could not get sucursale")
	}

	response, err := json.Marshal(sucursale)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("could not marshal sucursale response json")
	}

	return response, http.StatusOK, nil
}

func extractSucursalaParams(r *http.Request) (repositories.InsertSucursala, error) {
	var unmarshalledSucursala repositories.InsertSucursala

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return repositories.InsertSucursala{}, err
	}

	err = json.Unmarshal(body, &unmarshalledSucursala)
	if err != nil {
		return repositories.InsertSucursala{}, err
	}

	return unmarshalledSucursala, nil
}

func insertSucursala(r *http.Request, db datasources.DBClient, logger *log.Logger) (int, error) {
	sucursala, err := extractSucursalaParams(r)
	if err != nil {
		return http.StatusBadRequest, errors.New("sucursala information sent on request body does not match required format")
	}

	err = db.InsertSucursala(sucursala)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return http.StatusInternalServerError, errors.New("could not save sucursala")
	}

	return http.StatusOK, nil
}
