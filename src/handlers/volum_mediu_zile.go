package handlers

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"modbSalesApp/src/datasources"
	"modbSalesApp/src/repositories"
)

func HandleCantitateMedieZile(w http.ResponseWriter, r *http.Request, connections datasources.Connections, logger *log.Logger) {
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
		response, status, err = getCantitateMedieZile(db, r, logger)
	default:
		status = http.StatusBadRequest
		err = errors.New("wrong method type for /cantitateZile route")
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

func getCantitateMedieZile(db datasources.DBClient, r *http.Request, logger *log.Logger) ([]byte, int, error) {
	dataStart, err := getStringParameter(r, "DataStart", false)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	dataEnd, err := getStringParameter(r, "DataEnd", false)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	articole, err := db.GetCantitateLivrataZile(dataStart, dataEnd)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return nil, http.StatusInternalServerError, errors.New("could not get volumLivratZile")
	}

	response, err := json.Marshal(articole)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("could not marshal volumLivratZile response json")
	}

	return response, http.StatusOK, nil
}
