package handlers

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"modbSalesApp/src/datasources"
	"modbSalesApp/src/repositories"
)

func HandleFormReport(w http.ResponseWriter, r *http.Request, dw datasources.DBClient, logger *log.Logger) {
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
		response, status, err = getFormReport(dw, r, logger)
	default:
		status = http.StatusBadRequest
		err = errors.New("wrong method type for /formReport route")
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

func getFormReport(dw datasources.DBClient, r *http.Request, logger *log.Logger) ([]byte, int, error) {
	formParams, err := getFormParams(r)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	formReport, err := dw.GetFormReport(formParams)
	if err != nil {
		logger.Printf("Internal error: %s", err.Error())
		return nil, http.StatusInternalServerError, errors.New("could not get formReport")
	}

	response, err := json.Marshal(formReport)
	if err != nil {
		return nil, http.StatusInternalServerError, errors.New("could not marshal formReport response json")
	}

	return response, http.StatusOK, nil
}

func getFormParams(r *http.Request) (repositories.FormParams, error) {
	codVanzator, err := getIntParameter(r, "CodVanzator", false)
	if err != nil {
		return repositories.FormParams{}, err
	}
	numeArticol, err := getStringParameter(r, "NumeArticol", false)
	if err != nil {
		return repositories.FormParams{}, err
	}
	numePartener, err := getStringParameter(r, "NumePartener", false)
	if err != nil {
		return repositories.FormParams{}, err
	}
	numeSucursala, err := getStringParameter(r, "NumeSucursala", false)
	if err != nil {
		return repositories.FormParams{}, err
	}
	dataStart, err := getStringParameter(r, "DataStart", false)
	if err != nil {
		return repositories.FormParams{}, err
	}
	dataEnd, err := getStringParameter(r, "DataEnd", false)
	if err != nil {
		return repositories.FormParams{}, err
	}

	return repositories.FormParams{
		CodVanzator:   codVanzator,
		NumeArticol:   numeArticol,
		NumePartener:  numePartener,
		NumeSucursala: numeSucursala,
		DataStart:     dataStart,
		DataEnd:       dataEnd,
	}, nil
}
