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

func HandleProiecte(w http.ResponseWriter, r *http.Request, db datasources.DBClient, logger *log.Logger) {
	var response []byte
	var status int
	var err error

	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, X-CSRF-Token")
		w.Header().Set("Access-Control-Expose-Headers", "Authorization")
	case http.MethodGet:
		response, status, err = getProiecte(db, logger)
	case http.MethodPost:
		status, err = insertProiect(r, db, logger)
	default:
		status = http.StatusBadRequest
		err = errors.New("wrong method type for /proiecte route")
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

func getProiecte(db datasources.DBClient, logger *log.Logger) ([]byte, int, error) {
	proiecte, err := db.GetProiecte()
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return nil, http.StatusInternalServerError, errors.New("could not get proiecte")
	}

	response, err := json.Marshal(proiecte)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("could not marshal proiecte response json")
	}

	return response, http.StatusOK, nil
}

func extractProiectParams(r *http.Request) (repositories.Proiect, error) {
	var unmarshalledProiect repositories.Proiect

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return repositories.Proiect{}, err
	}

	err = json.Unmarshal(body, &unmarshalledProiect)
	if err != nil {
		return repositories.Proiect{}, err
	}

	return unmarshalledProiect, nil
}

func insertProiect(r *http.Request, db datasources.DBClient, logger *log.Logger) (int, error) {
	proiect, err := extractProiectParams(r)
	if err != nil {
		return http.StatusBadRequest, errors.New("proiect information sent on request body does not match required format")
	}

	err = db.InsertProiect(proiect)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return http.StatusInternalServerError, errors.New("could not save proiect")
	}

	return http.StatusOK, nil
}
