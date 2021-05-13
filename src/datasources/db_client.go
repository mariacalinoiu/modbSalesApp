package datasources

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/sijms/go-ora"

	"modbSalesApp/src/repositories"
)

type (
	DBClient struct {
		db          *sql.DB
		name        string
		tableSuffix string
	}

	Connections map[string]DBClient
)

const (
	GlobalConnectionName = "global"
	Local1ConnectionName = "local1"
	Local2ConnectionName = "local2"
	Local3ConnectionName = "local3"
	Local4ConnectionName = "local4"
)

func GetClient(user string, password string, hostname string, dbName string, connectionName string) DBClient {
	db, err := sql.Open(
		"oracle",
		fmt.Sprintf("oracle://%s:%s@%s/%s", user, password, hostname, dbName),
	)
	if err != nil {
		panic(err)
	}

	db.SetConnMaxLifetime(time.Minute * 3000)
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(100)

	return DBClient{
		db:          db,
		name:        connectionName,
		tableSuffix: getTableSuffix(connectionName),
	}
}

func (client DBClient) GetParteneri() ([]repositories.Partener, error) {
	var (
		parteneri []repositories.Partener
		cod       string
		nume      string
		cui       string
		email     string
		IDAdresa  int
	)

	switch client.name {
	default:
	case GlobalConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodPartener", "NumePartener", "CUI", "EMail", "IdAdresa" FROM "Parteneri%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Partener{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&cod, &nume, &cui, &email, &IDAdresa)
			if err != nil {
				return []repositories.Partener{}, err
			}

			parteneri = append(
				parteneri,
				repositories.Partener{
					CodPartener:  cod,
					NumePartener: nume,
					CUI:          cui,
					Email:        email,
					IDAdresa:     IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Partener{}, err
		}
		break

	case Local1ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodPartener", "NumePartener", "IdAdresa" FROM "Parteneri%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Partener{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&cod, &nume, &IDAdresa)
			if err != nil {
				return []repositories.Partener{}, err
			}

			parteneri = append(
				parteneri,
				repositories.Partener{
					CodPartener:  cod,
					NumePartener: nume,
					CUI:          cui,
					Email:        email,
					IDAdresa:     IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Partener{}, err
		}
		break

	case Local2ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodPartener", "CUI", "IdAdresa" FROM "Parteneri%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Partener{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&cod, &cui, &IDAdresa)
			if err != nil {
				return []repositories.Partener{}, err
			}

			parteneri = append(
				parteneri,
				repositories.Partener{
					CodPartener:  cod,
					NumePartener: nume,
					CUI:          cui,
					Email:        email,
					IDAdresa:     IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Partener{}, err
		}
		break

	case Local3ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodPartener", "EMail", "IdAdresa" FROM "Parteneri%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Partener{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&cod, &email, &IDAdresa)
			if err != nil {
				return []repositories.Partener{}, err
			}

			parteneri = append(
				parteneri,
				repositories.Partener{
					CodPartener:  cod,
					NumePartener: nume,
					CUI:          cui,
					Email:        email,
					IDAdresa:     IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Partener{}, err
		}
		break

	case Local4ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodPartener", "IdAdresa" FROM "Parteneri%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Partener{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&cod, &IDAdresa)
			if err != nil {
				return []repositories.Partener{}, err
			}

			parteneri = append(
				parteneri,
				repositories.Partener{
					CodPartener:  cod,
					NumePartener: nume,
					CUI:          cui,
					Email:        email,
					IDAdresa:     IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Partener{}, err
		}
		break

	}

	return parteneri, nil
}

func (client DBClient) InsertPartener(partenerAdresa repositories.InsertPartener) error {
	IDAdresa, err := client.InsertAdresa(partenerAdresa.Adresa)
	if err != nil {
		return err
	}

	partener := partenerAdresa.Partener
	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "Parteneri%s"("CodPartener", "NumePartener", "CUI", "EMail", "IdAdresa") VALUES(:1, :2, :3, :4, :5)`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		partener.CodPartener,
		partener.NumePartener,
		partener.CUI,
		partener.Email,
		IDAdresa,
	)

	return err
}

func (client DBClient) GetAdrese() ([]repositories.Adresa, error) {
	var (
		adrese     []repositories.Adresa
		IDAdresa   int
		numeAdresa string
		oras       string
		judet      string
		sector     string
		strada     string
		numar      string
		bloc       string
		etaj       int
	)

	switch client.name {
	default:
	case GlobalConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "IdAdresa", "NumeAdresa", "Oras", "Judet", "Sector", "Strada", "Numar", "Bloc", "Etaj" FROM "Adrese%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Adresa{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&IDAdresa, &numeAdresa, &oras, &judet, &sector, &strada, &numar, &bloc, &etaj)
			if err != nil {
				return []repositories.Adresa{}, err
			}

			adrese = append(
				adrese,
				repositories.Adresa{
					IDAdresa:   IDAdresa,
					NumeAdresa: numeAdresa,
					Oras:       oras,
					Judet:      judet,
					Sector:     sector,
					Strada:     strada,
					Numar:      numar,
					Bloc:       bloc,
					Etaj:       etaj,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Adresa{}, err
		}
		break

	case Local1ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "IdAdresa", "NumeAdresa" FROM "Adrese%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Adresa{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&IDAdresa, &numeAdresa)
			if err != nil {
				return []repositories.Adresa{}, err
			}

			adrese = append(
				adrese,
				repositories.Adresa{
					IDAdresa:   IDAdresa,
					NumeAdresa: numeAdresa,
					Oras:       oras,
					Judet:      judet,
					Sector:     sector,
					Strada:     strada,
					Numar:      numar,
					Bloc:       bloc,
					Etaj:       etaj,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Adresa{}, err
		}
		break

	case Local2ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "IdAdresa", "Oras" "Adrese%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Adresa{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&IDAdresa, &oras)
			if err != nil {
				return []repositories.Adresa{}, err
			}

			adrese = append(
				adrese,
				repositories.Adresa{
					IDAdresa:   IDAdresa,
					NumeAdresa: numeAdresa,
					Oras:       oras,
					Judet:      judet,
					Sector:     sector,
					Strada:     strada,
					Numar:      numar,
					Bloc:       bloc,
					Etaj:       etaj,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Adresa{}, err
		}
		break

	case Local3ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "IdAdresa", "Judet" FROM "Adrese%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Adresa{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&IDAdresa, &judet)
			if err != nil {
				return []repositories.Adresa{}, err
			}

			adrese = append(
				adrese,
				repositories.Adresa{
					IDAdresa:   IDAdresa,
					NumeAdresa: numeAdresa,
					Oras:       oras,
					Judet:      judet,
					Sector:     sector,
					Strada:     strada,
					Numar:      numar,
					Bloc:       bloc,
					Etaj:       etaj,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Adresa{}, err
		}
		break

	case Local4ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "IdAdresa", "Sector", "Strada", "Numar", "Bloc", "Etaj" FROM "Adrese%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Adresa{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&IDAdresa, &sector, &strada, &numar, &bloc, &etaj)
			if err != nil {
				return []repositories.Adresa{}, err
			}

			adrese = append(
				adrese,
				repositories.Adresa{
					IDAdresa:   IDAdresa,
					NumeAdresa: numeAdresa,
					Oras:       oras,
					Judet:      judet,
					Sector:     sector,
					Strada:     strada,
					Numar:      numar,
					Bloc:       bloc,
					Etaj:       etaj,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Adresa{}, err
		}
		break

	}

	return adrese, nil
}

func (client DBClient) InsertAdresa(adresa repositories.Adresa) (int, error) {
	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "Adrese%s"("NumeAdresa", "Oras", "Judet", "Sector", "Strada", "Numar", "Bloc", "Etaj") VALUES(:1, :2, :3, :4, :5, :6, :7, :8)`, client.tableSuffix))
	if err != nil {
		return -1, err
	}

	_, err = stmt.Exec(
		adresa.NumeAdresa,
		adresa.Oras,
		adresa.Judet,
		adresa.Sector,
		adresa.Strada,
		adresa.Numar,
		adresa.Bloc,
		adresa.Etaj,
	)
	if err != nil {
		return -1, err
	}

	var IDAdresa int
	rows, err := client.db.Query(fmt.Sprintf(`SELECT NVL(MAX("IdAdresa"), 0) FROM "Adrese%s"`, client.tableSuffix))
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	rows.Next()
	err = rows.Scan(&IDAdresa)

	return IDAdresa, err
}

func (client DBClient) GetVanzari() ([]repositories.Vanzare, error) {
	var (
		vanzari     []repositories.Vanzare
		id          int
		codPartener string
		status      string
		data        string
		dataLivrare string
		total       float32
		vat         float32
		discount    float32
		moneda      string
		platit      float32
		comentarii  string
		codVanzator int
		IDSucursala int
	)

	rows, err := client.db.Query(
		fmt.Sprintf(`
			SELECT "IdIntrare", "CodPartener", "Status", "Data", "DataLivrare", "Total", "Vat", "Discount", "Moneda", "Platit", NVL("Comentarii", 'N/A'), "CodVanzator", "IdSucursala" 
			FROM "Vanzari%s" 
			ORDER BY "IdIntrare"
			OFFSET 0 ROWS FETCH NEXT 15 ROWS ONLY
		`, client.tableSuffix),
	)
	if err != nil {
		return []repositories.Vanzare{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &codPartener, &status, &data, &dataLivrare, &total, &vat, &discount, &moneda, &platit, &comentarii, &codVanzator, &IDSucursala)
		if err != nil {
			return []repositories.Vanzare{}, err
		}

		vanzari = append(
			vanzari,
			repositories.Vanzare{
				IDIntrare:   id,
				CodPartener: codPartener,
				Status:      status,
				Data:        data,
				DataLivrare: dataLivrare,
				Total:       total,
				VAT:         vat,
				Discount:    discount,
				Moneda:      moneda,
				Platit:      platit,
				Comentarii:  comentarii,
				CodVanzator: codVanzator,
				IDSucursala: IDSucursala,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.Vanzare{}, err
	}

	return vanzari, nil
}

func (client DBClient) InsertVanzare(vanzareLinii repositories.InsertVanzare) error {
	vanzare := vanzareLinii.Vanzare
	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "Vanzari%s"("CodPartener", "Status", "Data", "DataLivrare", "Total", "Vat", "Discount", "Moneda", "Platit", "Comentarii", "CodVanzator", "IdSucursala") VALUES(:1, :2, TO_DATE(:3, 'MM/DD/YYYY'), TO_DATE(:4, 'MM/DD/YYYY'), :5, :6, :7, :8, :9, :10, :11, :12)`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		vanzare.CodPartener,
		vanzare.Status,
		vanzare.Data,
		vanzare.DataLivrare,
		vanzare.Total,
		vanzare.VAT,
		vanzare.Discount,
		vanzare.Moneda,
		vanzare.Platit,
		vanzare.Comentarii,
		vanzare.CodVanzator,
		vanzare.IDSucursala,
	)
	if err != nil {
		return err
	}

	var IDIntrare int
	rows, err := client.db.Query(fmt.Sprintf(`SELECT NVL(MAX("IdIntrare"), 0) FROM "Vanzari%s"`, client.tableSuffix))
	if err != nil {
		return err
	}
	defer rows.Close()
	rows.Next()
	err = rows.Scan(&IDIntrare)

	for _, linie := range vanzareLinii.LiniiVanzari {
		linie.IDIntrare = IDIntrare
		err = client.InsertLinieVanzare(linie)
		if err != nil {
			return err
		}
	}

	return nil
}

func (client DBClient) GetLiniiVanzare(IDIntrareVanzari int) ([]repositories.LinieVanzare, error) {
	var (
		liniiVanzare []repositories.LinieVanzare
		IDIntrare    int
		numarLinie   int
		codArticol   string
		cantitate    float32
		pret         float32
		discount     float32
		VAT          float32
		totalLinie   float32
		IDProiect    string
	)

	rows, err := client.db.Query(
		fmt.Sprintf(`SELECT "IdIntrare", "NumarLinie", "CodArticol", "Cantitate", "Pret", "Discount", "Vat", "TotalLinie", "IdProiect" FROM "LiniiVanzari%s" WHERE "IdIntrare" = :1`, client.tableSuffix),
		IDIntrareVanzari,
	)
	if err != nil {
		return []repositories.LinieVanzare{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&IDIntrare, &numarLinie, &codArticol, &cantitate, &pret, &discount, &VAT, &totalLinie, &IDProiect)
		if err != nil {
			return []repositories.LinieVanzare{}, err
		}

		liniiVanzare = append(
			liniiVanzare,
			repositories.LinieVanzare{
				IDIntrare:  IDIntrare,
				NumarLinie: numarLinie,
				CodArticol: codArticol,
				Cantitate:  cantitate,
				Pret:       pret,
				Discount:   discount,
				VAT:        VAT,
				TotalLinie: totalLinie,
				IDProiect:  IDProiect,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.LinieVanzare{}, err
	}

	return liniiVanzare, nil
}

func (client DBClient) InsertLinieVanzare(linie repositories.LinieVanzare) error {
	var nrLinieVanzare int
	rows, err := client.db.Query(
		fmt.Sprintf(`SELECT NVL(MAX("NumarLinie"), 0) FROM "LiniiVanzari%s" WHERE "IdIntrare" = :1`, client.tableSuffix),
		linie.IDIntrare,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	rows.Next()
	err = rows.Scan(&nrLinieVanzare)

	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "LiniiVanzari%s"("IdIntrare", "NumarLinie", "CodArticol", "Cantitate", "Pret", "Discount", "Vat", "TotalLinie", "IdProiect") VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9)`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		linie.IDIntrare,
		nrLinieVanzare+1,
		linie.CodArticol,
		linie.Cantitate,
		linie.Pret,
		linie.Discount,
		linie.VAT,
		linie.TotalLinie,
		linie.IDProiect,
	)

	return err
}

func (client DBClient) EditLinieVanzare(linie repositories.LinieVanzare) error {
	stmt, err := client.db.Prepare(fmt.Sprintf(`UPDATE "LiniiVanzari%s" SET "CodArticol" = :1, "Cantitate" = :2, "Pret" = :3, "Discount" = :4, "Vat" = :5, "TotalLinie" = :6, "IdProiect" = :7 WHERE "IdIntrare" = :8 AND "NumarLinie" = :9`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		linie.CodArticol,
		linie.Cantitate,
		linie.Pret,
		linie.Discount,
		linie.VAT,
		linie.TotalLinie,
		linie.IDProiect,
		linie.IDIntrare,
		linie.NumarLinie,
	)

	return err
}

func (client DBClient) DeleteLinieVanzare(IDIntrare int, numarLinie int) error {
	stmt, err := client.db.Prepare(fmt.Sprintf(`DELETE FROM "LiniiVanzari%s" WHERE "IdIntrare" = :1 AND "NumarLinie" = :2`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		IDIntrare,
		numarLinie,
	)

	return err
}

func (client DBClient) GetArticole() ([]repositories.Articol, error) {
	var (
		articole        []repositories.Articol
		cod             string
		nume            string
		codGrupa        int
		cantitateStoc   int
		IDUnitateMasura int
	)

	rows, err := client.db.Query(
		fmt.Sprintf(`SELECT "CodArticol", "NumeArticol", "CodGrupa", "CantitateStoc", "IdUnitateDeMasura" FROM "Articole%s"`, client.tableSuffix),
	)
	if err != nil {
		return []repositories.Articol{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cod, &nume, &codGrupa, &cantitateStoc, &IDUnitateMasura)
		if err != nil {
			return []repositories.Articol{}, err
		}

		articole = append(
			articole,
			repositories.Articol{
				CodArticol:      cod,
				NumeArticol:     nume,
				CodGrupa:        codGrupa,
				CantitateStoc:   cantitateStoc,
				IDUnitateMasura: IDUnitateMasura,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.Articol{}, err
	}

	return articole, nil
}

func (client DBClient) InsertArticol(articol repositories.Articol) error {
	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "Articole%s"("CodArticol", "NumeArticol", "CodGrupa", "CantitateStoc", "IdUnitateDeMasura") VALUES(:1, :2, :3, :4, :5)`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		articol.CodArticol,
		articol.NumeArticol,
		articol.CodGrupa,
		articol.CantitateStoc,
		articol.IDUnitateMasura,
	)

	return err
}

func (client DBClient) GetVanzatori() ([]repositories.Vanzator, error) {
	var (
		vanzatori   []repositories.Vanzator
		codVanzator int
		nume        string
		prenume     string
		salariuBaza float32
		comision    float32
		email       string
		IDAdresa    int
	)

	switch client.name {
	default:
	case GlobalConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodVanzator", "Nume", "Prenume", "SalariuBaza", "Comision", "EMail", "IdAdresa" FROM "Vanzatori%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Vanzator{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&codVanzator, &nume, &prenume, &salariuBaza, &comision, &email, &IDAdresa)
			if err != nil {
				return []repositories.Vanzator{}, err
			}

			vanzatori = append(
				vanzatori,
				repositories.Vanzator{
					CodVanzator: codVanzator,
					Nume:        nume,
					Prenume:     prenume,
					SalariuBaza: salariuBaza,
					Comision:    comision,
					Email:       email,
					IDAdresa:    IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Vanzator{}, err
		}
		break

	case Local1ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodVanzator", "Nume", "Prenume", "IdAdresa" FROM "Vanzatori%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Vanzator{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&codVanzator, &nume, &prenume, &IDAdresa)
			if err != nil {
				return []repositories.Vanzator{}, err
			}

			vanzatori = append(
				vanzatori,
				repositories.Vanzator{
					CodVanzator: codVanzator,
					Nume:        nume,
					Prenume:     prenume,
					SalariuBaza: salariuBaza,
					Comision:    comision,
					Email:       email,
					IDAdresa:    IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Vanzator{}, err
		}
		break

	case Local2ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodVanzator", "SalariuBaza", "IdAdresa" FROM "Vanzatori%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Vanzator{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&codVanzator, &salariuBaza, &IDAdresa)
			if err != nil {
				return []repositories.Vanzator{}, err
			}

			vanzatori = append(
				vanzatori,
				repositories.Vanzator{
					CodVanzator: codVanzator,
					Nume:        nume,
					Prenume:     prenume,
					SalariuBaza: salariuBaza,
					Comision:    comision,
					Email:       email,
					IDAdresa:    IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Vanzator{}, err
		}
		break

	case Local3ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodVanzator", "Comision", "IdAdresa" FROM "Vanzatori%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Vanzator{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&codVanzator, &comision, &IDAdresa)
			if err != nil {
				return []repositories.Vanzator{}, err
			}

			vanzatori = append(
				vanzatori,
				repositories.Vanzator{
					CodVanzator: codVanzator,
					Nume:        nume,
					Prenume:     prenume,
					SalariuBaza: salariuBaza,
					Comision:    comision,
					Email:       email,
					IDAdresa:    IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Vanzator{}, err
		}
		break

	case Local4ConnectionName:
		rows, err := client.db.Query(
			fmt.Sprintf(`SELECT "CodVanzator", "EMail", FROM "Vanzatori%s"`, client.tableSuffix),
		)
		if err != nil {
			return []repositories.Vanzator{}, err
		}

		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&codVanzator, &nume, &prenume, &salariuBaza, &comision, &email, &IDAdresa)
			if err != nil {
				return []repositories.Vanzator{}, err
			}

			vanzatori = append(
				vanzatori,
				repositories.Vanzator{
					CodVanzator: codVanzator,
					Nume:        nume,
					Prenume:     prenume,
					SalariuBaza: salariuBaza,
					Comision:    comision,
					Email:       email,
					IDAdresa:    IDAdresa,
				},
			)
		}

		err = rows.Err()
		if err != nil {
			return []repositories.Vanzator{}, err
		}
		break

	}

	return vanzatori, nil
}

func (client DBClient) InsertVanzator(vanzatorAdresa repositories.InsertVanzator) error {
	IDAdresa, err := client.InsertAdresa(vanzatorAdresa.Adresa)
	if err != nil {
		return err
	}

	vanzator := vanzatorAdresa.Vanzator
	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "Vanzatori%s"("Nume", "Prenume", "SalariuBaza", "Comision", "EMail", "IdAdresa") VALUES(:1, :2, :3, :4, :5, :6)`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		vanzator.Nume,
		vanzator.Prenume,
		vanzator.SalariuBaza,
		vanzator.Comision,
		vanzator.Email,
		IDAdresa,
	)

	return err
}

func (client DBClient) GetSucursale() ([]repositories.Sucursala, error) {
	var (
		sucursale   []repositories.Sucursala
		IDSucursala int
		nume        string
		IDAdresa    int
	)

	rows, err := client.db.Query(
		fmt.Sprintf(`SELECT "IdSucursala", "NumeSucursala", "IdAdresa" FROM "Sucursale%s"`, client.tableSuffix),
	)
	if err != nil {
		return []repositories.Sucursala{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&IDSucursala, &nume, &IDAdresa)
		if err != nil {
			return []repositories.Sucursala{}, err
		}

		sucursale = append(
			sucursale,
			repositories.Sucursala{
				IDSucursala:   IDSucursala,
				NumeSucursala: nume,
				IDAdresa:      IDAdresa,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.Sucursala{}, err
	}

	return sucursale, nil
}

func (client DBClient) InsertSucursala(sucursalaAdresa repositories.InsertSucursala, global DBClient) error {
	IDAdresa, err := global.InsertAdresa(sucursalaAdresa.Adresa)
	if err != nil {
		return err
	}

	sucursala := sucursalaAdresa.Sucursala

	var IDSucursala int
	rows, err := global.db.Query(fmt.Sprintf(`SELECT NVL(MAX("IdSucursala"), 0) FROM "Sucursale%s"`, global.tableSuffix))
	if err != nil {
		return err
	}
	defer rows.Close()
	rows.Next()
	err = rows.Scan(&IDSucursala)

	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "Sucursale%s"("IdSucursala", NumeSucursala", "IdAdresa") VALUES(:1, :2, :3)`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		IDSucursala+1,
		sucursala.NumeSucursala,
		IDAdresa,
	)

	return err
}

func (client DBClient) GetProiecte() ([]repositories.Proiect, error) {
	var (
		proiecte    []repositories.Proiect
		IDProiect   string
		nume        string
		validDeLa   string
		validPanaLa string
		activ       string
	)

	rows, err := client.db.Query(
		fmt.Sprintf(`SELECT "IdProiect", "NumeProiect", "ValidDeLa", "ValidPanaLa", "Activ" FROM "Proiecte%s"`, client.tableSuffix),
	)
	if err != nil {
		return []repositories.Proiect{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&IDProiect, &nume, &validDeLa, &validPanaLa, &activ)
		if err != nil {
			return []repositories.Proiect{}, err
		}

		proiecte = append(
			proiecte,
			repositories.Proiect{
				IDProiect:   IDProiect,
				NumeProiect: nume,
				ValidDeLa:   validDeLa,
				ValidPanaLa: validPanaLa,
				Activ:       activ,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.Proiect{}, err
	}

	return proiecte, nil
}

func (client DBClient) InsertProiect(proiect repositories.Proiect) error {
	stmt, err := client.db.Prepare(fmt.Sprintf(`INSERT INTO "Proiecte%s"("IdProiect", "NumeProiect", "ValidDeLa", "ValidPanaLa", "Activ") VALUES(:1, :2, TO_DATE(:3, 'MM/DD/YYYY'), TO_DATE(:4, 'MM/DD/YYYY'), :5)`, client.tableSuffix))
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
		proiect.IDProiect,
		proiect.NumeProiect,
		proiect.ValidDeLa,
		proiect.ValidPanaLa,
		proiect.Activ,
	)

	return err
}

func (client DBClient) GetGrupeArticole() ([]repositories.GrupaArticole, error) {
	var (
		grupe   []repositories.GrupaArticole
		cod     int
		nume    string
		detalii string
	)

	rows, err := client.db.Query(
		fmt.Sprintf(`SELECT "CodGrupa", "NumeGrupa", NVL("DetaliiGrupa", ' ') FROM "GrupaArticole%s"`, client.tableSuffix),
	)
	if err != nil {
		return []repositories.GrupaArticole{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cod, &nume, &detalii)
		if err != nil {
			return []repositories.GrupaArticole{}, err
		}

		grupe = append(
			grupe,
			repositories.GrupaArticole{
				CodGrupa:     cod,
				NumeGrupa:    nume,
				DetaliiGrupa: detalii,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.GrupaArticole{}, err
	}

	return grupe, nil
}

func (client DBClient) GetUnitatiDeMasura() ([]repositories.UnitateDeMasura, error) {
	var (
		um       []repositories.UnitateDeMasura
		id       int
		nume     string
		inaltime float32
		latime   float32
		lungime  float32
	)

	rows, err := client.db.Query(
		fmt.Sprintf(`SELECT "IdUnitateDeMasura", "NumeUnitateDeMasura", "Inaltime", "Latime", "Lungime" FROM "UnitatiDeMasura%s"`, client.tableSuffix),
	)
	if err != nil {
		return []repositories.UnitateDeMasura{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &nume, &inaltime, &latime, &lungime)
		if err != nil {
			return []repositories.UnitateDeMasura{}, err
		}

		um = append(
			um,
			repositories.UnitateDeMasura{
				IDUnitateMasura:     id,
				NumeUnitateDeMasura: nume,
				Inaltime:            inaltime,
				Latime:              latime,
				Lungime:             lungime,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.UnitateDeMasura{}, err
	}

	return um, nil
}

func (client DBClient) GetVanzariGrupeArticole() ([]repositories.VanzariGrupeArticole, error) {
	var (
		results       []repositories.VanzariGrupeArticole
		numeGrupa     string
		vanzareTotala float32
	)

	rows, err := client.db.Query(fmt.Sprintf(`
		SELECT NVL(SUM(v."Platit"), 0) VanzareTotala, ga."NumeGrupa"
		FROM "Vanzari%s" v, "LiniiVanzari%s" lv, "Articole%s" a, "GrupaArticole%s" ga
		WHERE v."IdIntrare" = lv."IdIntrare" AND lv."CodArticol" = a."CodArticol" AND a."CodGrupa" = ga."CodGrupa"
		GROUP BY ga."NumeGrupa"
		
	`, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix))
	if err != nil {
		return []repositories.VanzariGrupeArticole{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&vanzareTotala, &numeGrupa)
		if err != nil {
			return []repositories.VanzariGrupeArticole{}, err
		}

		results = append(
			results,
			repositories.VanzariGrupeArticole{
				NumeGrupa:     numeGrupa,
				VanzareTotala: vanzareTotala,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.VanzariGrupeArticole{}, err
	}

	return results, nil
}

func (client DBClient) GetCantitatiJudete() ([]repositories.CantitateJudete, error) {
	var (
		results        []repositories.CantitateJudete
		judet          string
		um             string
		cantitateMedie float32
	)

	query := fmt.Sprintf(`
		SELECT (
			SELECT NVL(AVG(SUM(lv."Cantitate")), 0)
			FROM "Vanzari%s" v, "LiniiVanzari%s" lv, "Sucursale%s" s, "Adrese%s" ad2, "Articole%s" ar, "UnitatiDeMasura%s" um2 
			WHERE v."IdIntrare" = lv."IdIntrare" AND v."IdSucursala" = s."IdSucursala" AND s."IdAdresa" = ad2."IdAdresa" AND lv."CodArticol" = ar."CodArticol" AND ar."IdUnitateDeMasura" = um2."IdUnitateDeMasura"
			AND um2."NumeUnitateDeMasura" = um."NumeUnitateDeMasura" AND ad2."Judet" = ad."Judet"
			GROUP BY um2."NumeUnitateDeMasura", ad2."Judet"
		) CantitateMedie, um."NumeUnitateDeMasura", ad."Judet"
		FROM "Vanzari%s" v, "LiniiVanzari%s" lv, "Sucursale%s" s, "Adrese%s" ad, "Articole%s" ar, "UnitatiDeMasura%s" um
		WHERE v."IdIntrare" = lv."IdIntrare" AND v."IdSucursala" = s."IdSucursala" AND s."IdAdresa" = ad."IdAdresa" AND lv."CodArticol" = ar."CodArticol" AND ar."IdUnitateDeMasura" = um."IdUnitateDeMasura"
		GROUP BY um."NumeUnitateDeMasura", ad."Judet"
	`, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix, client.tableSuffix)
	fmt.Println(query)

	rows, err := client.db.Query(query)
	if err != nil {
		return []repositories.CantitateJudete{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cantitateMedie, &um, &judet)
		if err != nil {
			return []repositories.CantitateJudete{}, err
		}

		results = append(
			results,
			repositories.CantitateJudete{
				Judet:          judet,
				Um:             um,
				CantitateMedie: cantitateMedie,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.CantitateJudete{}, err
	}

	return results, nil
}

func (client DBClient) GetProcentDiscountTrimestre() ([]repositories.ProcentDiscountTrimestru, error) {
	var (
		results         []repositories.ProcentDiscountTrimestru
		trimestru       string
		procentDiscount float32
	)

	query := fmt.Sprintf(`
		SELECT NVL(AVG("Discount" * 100 / NVL("Total", 1)), 0) ProcentDiscount, EXTRACT(YEAR FROM "Data") || '-q' || (
			CASE 
				WHEN EXTRACT(MONTH FROM "Data") IN (1, 2, 3) THEN 1
				WHEN EXTRACT(MONTH FROM "Data") IN (4, 5, 6) THEN 2
				WHEN EXTRACT(MONTH FROM "Data") IN (7, 8, 9) THEN 3
				WHEN EXTRACT(MONTH FROM "Data") IN (10, 11, 12) THEN 4
			END
		) Trimestru
		FROM "Vanzari%s"
		GROUP BY EXTRACT(YEAR FROM "Data") || '-q' || (
			CASE 
				WHEN EXTRACT(MONTH FROM "Data") IN (1, 2, 3) THEN 1
				WHEN EXTRACT(MONTH FROM "Data") IN (4, 5, 6) THEN 2
				WHEN EXTRACT(MONTH FROM "Data") IN (7, 8, 9) THEN 3
				WHEN EXTRACT(MONTH FROM "Data") IN (10, 11, 12) THEN 4
			END
		)
		ORDER BY Trimestru
	`, client.tableSuffix)
	fmt.Println(query)

	rows, err := client.db.Query(query)
	if err != nil {
		return []repositories.ProcentDiscountTrimestru{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&procentDiscount, &trimestru)
		if err != nil {
			return []repositories.ProcentDiscountTrimestru{}, err
		}

		results = append(
			results,
			repositories.ProcentDiscountTrimestru{
				Trimestru:       trimestru,
				ProcentDiscount: procentDiscount,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.ProcentDiscountTrimestru{}, err
	}

	return results, nil
}

func (client DBClient) GetCantitateLivrataZile(dataStart string, dataEnd string) ([]repositories.CantitateLivrataZile, error) {
	var (
		results               []repositories.CantitateLivrataZile
		ziSaptamana           string
		cantitateMedieLivrata float32
	)

	whereStatement := ""
	if len(dataStart) > 0 {
		whereStatement = fmt.Sprintf("%s%s%s", `WHERE v."Data" >= TO_DATE('`, dataStart, `', 'MM/DD/YYYY')`)
	}
	if len(dataEnd) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = fmt.Sprintf("%s%s%s", `WHERE v."Data" <= TO_DATE('`, dataEnd, `', 'MM/DD/YYYY')`)
		} else {
			whereStatement = fmt.Sprintf("%s%s%s%s", whereStatement, ` AND v."Data" <= TO_DATE(`, dataEnd, `, 'MM/DD/YYYY')`)
		}
	}

	subQueryWhere := whereStatement
	subQueryWhere = strings.Replace(subQueryWhere, "v.", "v2.", -1)
	if len(subQueryWhere) == 0 {
		subQueryWhere = `WHERE v2."IdIntrare" = lv2."IdIntrare" AND TO_CHAR(v2."DataLivrare", 'DY') = TO_CHAR(v."DataLivrare", 'DY')`
	} else {
		subQueryWhere = fmt.Sprintf(`%s AND v2."IdIntrare" = lv2."IdIntrare" AND TO_CHAR(v2."DataLivrare", 'DY') = TO_CHAR(v."DataLivrare", 'DY')`, subQueryWhere)
	}

	query := fmt.Sprintf("%s\n%s\n%s",
		fmt.Sprintf(`
		SELECT (
			SELECT NVL(AVG(SUM(lv2."Cantitate")), 0)
			FROM "Vanzari%s" v2, "LiniiVanzari%s" lv2
			%s
			GROUP BY TO_CHAR(v2."DataLivrare", 'DY')
		) CantitateMedieLivrata, TO_CHAR(v."DataLivrare", 'DY') ZiSaptamana
		FROM "Vanzari%s" v
		`, client.tableSuffix, client.tableSuffix, subQueryWhere, client.tableSuffix),
		whereStatement,
		`GROUP BY TO_CHAR(v."DataLivrare", 'DY')`,
	)
	fmt.Println(query)

	rows, err := client.db.Query(query)
	if err != nil {
		return []repositories.CantitateLivrataZile{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&cantitateMedieLivrata, &ziSaptamana)
		if err != nil {
			return []repositories.CantitateLivrataZile{}, err
		}

		results = append(
			results,
			repositories.CantitateLivrataZile{
				ZiSaptamana:      ziSaptamana,
				VolumMediuLivrat: cantitateMedieLivrata,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.CantitateLivrataZile{}, err
	}

	return results, nil
}

func (client DBClient) GetFormReport(params repositories.FormParams) ([]repositories.FormResult, error) {
	selectStatement := `SELECT SUM(lv."Pret") pret, SUM(lv."Cantitate") cantitate, v."Vat", SUM(lv."Discount") discount, v."Platit", COUNT(*) numarTranzactii`
	fromStatement := fmt.Sprintf(`FROM "Vanzari%s" v, "LiniiVanzari%s" lv`, client.tableSuffix, client.tableSuffix)
	groupByStatement := `GROUP BY v."Vat", v."Platit", v."IdIntrare"`

	query, _ := client.getReportQueryBasedOnFormParams(selectStatement, fromStatement, groupByStatement, params)
	fmt.Println(query)

	var (
		results         []repositories.FormResult
		pret            float32
		cantitate       float32
		vat             float32
		discount        float32
		platit          float32
		numarTranzactii float32
	)

	rows, err := client.db.Query(query)
	if err != nil {
		return []repositories.FormResult{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&pret, &cantitate, &vat, &discount, &platit, &numarTranzactii)
		if err != nil {
			return []repositories.FormResult{}, err
		}

		results = append(
			results,
			repositories.FormResult{
				Pret:            pret,
				Cantitate:       cantitate,
				Vat:             vat,
				Discount:        discount,
				Platit:          platit,
				NumarTranzactii: numarTranzactii,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.FormResult{}, err
	}

	return results, nil
}

func (client DBClient) GetGroupedFormReport(params repositories.FormParams) ([]repositories.FormResult, error) {
	placeholder := "{PLACEHOLDER}"
	selectStatement := fmt.Sprintf(`
		SELECT NVL(SUM(lv."Pret"), 0) PretTotal, NVL(SUM(lv."Cantitate"), 0) CantitateTotal, NVL(SUM(v."Vat"), 0) VatTotal, 
			NVL(SUM(lv."Discount"), 0) DiscountTotal, NVL(SUM(v."Platit"), 0) PlatitTotal, 
			NVL(SUM(
				(SELECT COUNT(*) FROM "Vanzari%s" v, "LiniiVanzari%s" lv %s)
			), 0) NumarTranzactiiTotal,
			NVL(AVG(lv."Pret"), 0) PretMediu, NVL(AVG(lv."Cantitate"), 0) CantitateMedie, NVL(AVG(v."Vat"), 0) VatMediu, 
			NVL(AVG(lv."Discount"), 0) DiscountMediu, NVL(AVG(v."Platit"), 0) PlatitMedie, 
			NVL(AVG(
				(SELECT COUNT(*) FROM "Vanzari%s" v, "LiniiVanzari%s" lv %s)
			), 0) NumarTranzactiiMediu 
	`, client.tableSuffix, client.tableSuffix, placeholder, client.tableSuffix, client.tableSuffix, placeholder)
	fromStatement := fmt.Sprintf(`FROM "Vanzari%s" v, "LiniiVanzari%s" lv`, client.tableSuffix, client.tableSuffix)
	groupByStatement := `GROUP BY lv."IdIntrare"`

	query, where := client.getReportQueryBasedOnFormParams(selectStatement, fromStatement, groupByStatement, params)
	query = strings.Replace(query, placeholder, where, 2)
	fmt.Println(query)

	var (
		results              []repositories.FormResult
		pretTotal            float32
		cantitateTotal       float32
		vatTotal             float32
		discountTotal        float32
		platitTotal          float32
		numarTranzactiiTotal float32
		pretMediu            float32
		cantitateMedie       float32
		vatMediu             float32
		discountMediu        float32
		platitMedie          float32
		numarTranzactiiMediu float32
	)

	rows, err := client.db.Query(query)
	if err != nil {
		return []repositories.FormResult{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&pretTotal, &cantitateTotal, &vatTotal, &discountTotal, &platitTotal, &numarTranzactiiTotal,
			&pretMediu, &cantitateMedie, &vatMediu, &discountMediu, &platitMedie, &numarTranzactiiMediu)
		if err != nil {
			return []repositories.FormResult{}, err
		}

		results = append(
			results,
			repositories.FormResult{
				Pret:            pretTotal,
				Cantitate:       cantitateTotal,
				Vat:             vatTotal,
				Discount:        discountTotal,
				Platit:          platitTotal,
				NumarTranzactii: numarTranzactiiTotal,
			},
			repositories.FormResult{
				Pret:            pretMediu,
				Cantitate:       cantitateMedie,
				Vat:             vatMediu,
				Discount:        discountMediu,
				Platit:          platitMedie,
				NumarTranzactii: numarTranzactiiMediu,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.FormResult{}, err
	}

	return results, nil
}

func (client DBClient) getReportQueryBasedOnFormParams(selectStatement string, fromStatement string, groupByStatement string, params repositories.FormParams) (string, string) {
	whereStatement := ""

	if params.CodVanzator != 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s %d", whereStatement, `v."CodVanzator" =`, params.CodVanzator)
	}

	if len(params.NumeArticol) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s '%s'", whereStatement, `lv."CodArticol" = a."CodArticol" AND a."NumeArticol" =`, params.NumeArticol)
		fromStatement = fmt.Sprintf(`%s, "Articole%s" a`, fromStatement, client.tableSuffix)
	}

	if len(params.NumeSucursala) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s '%s'", whereStatement, `v."IdSucursala" = s."IdSucursala" AND s."NumeSucursala" =`, params.NumeSucursala)
		fromStatement = fmt.Sprintf(`%s, "Sucursale%s" s`, fromStatement, client.tableSuffix)
	}

	if len(params.NumePartener) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s '%s'", whereStatement, `v."CodPartener" = p."CodPartener" AND p."NumePartener" =`, params.NumePartener)
		fromStatement = fmt.Sprintf(`%s, "Parteneri%s" p`, fromStatement, client.tableSuffix)
	}

	if len(params.DataStart) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s'%s'%s", whereStatement, `v."Data" >= TO_DATE(`, params.DataStart, `, 'MM/DD/YYYY')`)
	}

	if len(params.DataEnd) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s'%s'%s", whereStatement, `v."Data" <= TO_DATE(`, params.DataEnd, `, 'MM/DD/YYYY')`)
	}

	if len(whereStatement) == 0 {
		whereStatement = `WHERE v."IdIntrare" = lv."IdIntrare"`
	} else {
		whereStatement = fmt.Sprintf(`%s AND v."IdIntrare" = lv."IdIntrare"`, whereStatement)
	}

	return fmt.Sprintf("%s\n%s\n%s\n%s", selectStatement, fromStatement, whereStatement, groupByStatement), whereStatement
}

func getTableSuffix(connectionName string) string {
	switch connectionName {
	default:
	case GlobalConnectionName:
		return ""
	case Local1ConnectionName:
		return "_S1"
	case Local2ConnectionName:
		return "_S2"
	case Local3ConnectionName:
		return "_S3"
	case Local4ConnectionName:
		return "_S4"
	}

	return ""
}
