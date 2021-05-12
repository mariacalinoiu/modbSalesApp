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

func HandleLiniiVanzari(w http.ResponseWriter, r *http.Request, db datasources.DBClient, logger *log.Logger) {
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
		response, status, err = getLiniiVanzari(r, db, logger)
	case http.MethodPost, http.MethodPut:
		status, err = insertLinieVanzare(r, db, logger, r.Method == http.MethodPut)
	case http.MethodDelete:
		status, err = deleteLinieVanzare(r, db, logger)
	default:
		status = http.StatusBadRequest
		err = errors.New("wrong method type for /liniiVanzari route")
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

func getLiniiVanzari(r *http.Request, db datasources.DBClient, logger *log.Logger) ([]byte, int, error) {
	IDIntrare, err := getIntParameter(r, "IDIntrare", true)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	vanzari, err := db.GetLiniiVanzare(IDIntrare)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return nil, http.StatusInternalServerError, errors.New("could not get linii vanzare")
	}

	response, err := json.Marshal(vanzari)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("could not marshal linii vanzare response json")
	}

	return response, http.StatusOK, nil
}

func extractLinieVanzareParams(r *http.Request) (repositories.LinieVanzare, error) {
	var unmarshalledVanzare repositories.LinieVanzare

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return repositories.LinieVanzare{}, err
	}

	err = json.Unmarshal(body, &unmarshalledVanzare)
	if err != nil {
		return repositories.LinieVanzare{}, err
	}

	return unmarshalledVanzare, nil
}

func insertLinieVanzare(r *http.Request, db datasources.DBClient, logger *log.Logger, update bool) (int, error) {
	linieVanzare, err := extractLinieVanzareParams(r)
	if err != nil {
		return http.StatusBadRequest, errors.New("linieVanzare information sent on request body does not match required format")
	}

	if update {
		err = db.EditLinieVanzare(linieVanzare)
	} else {
		err = db.InsertLinieVanzare(linieVanzare)
	}
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return http.StatusInternalServerError, errors.New("could not save linieVanzare")
	}

	return http.StatusOK, nil
}

func deleteLinieVanzare(r *http.Request, db datasources.DBClient, logger *log.Logger) (int, error) {
	IDIntrare, err := getIntParameter(r, "IDIntrare", true)
	if err != nil {
		return http.StatusBadRequest, err
	}
	numarLinie, err := getIntParameter(r, "NumarLinie", true)
	if err != nil {
		return http.StatusBadRequest, err
	}

	err = db.DeleteLinieVanzare(IDIntrare, numarLinie)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return http.StatusInternalServerError, errors.New("could not delete linieVanzare")
	}

	return http.StatusOK, nil
}
