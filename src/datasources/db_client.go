package datasources

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/sijms/go-ora"

	"modbSalesApp/src/repositories"
)

type DBClient struct {
	db *sql.DB
}

func GetClient(user string, password string, hostname string, dbName string) DBClient {
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

	return DBClient{db: db}
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

	rows, err := client.db.Query(
		`SELECT "CodPartener", "NumePartener", "CUI", "EMail", "IdAdresa" FROM "Parteneri"`,
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

	return parteneri, nil
}

func (client DBClient) InsertPartener(partenerAdresa repositories.InsertPartener) error {
	IDAdresa, err := client.InsertAdresa(partenerAdresa.Adresa)
	if err != nil {
		return err
	}

	partener := partenerAdresa.Partener
	stmt, err := client.db.Prepare(`INSERT INTO "Parteneri"("CodPartener", "NumePartener", "CUI", "EMail", "IdAdresa") VALUES(:1, :2, :3, :4, :5)`)
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

func (client DBClient) InsertAdresa(adresa repositories.Adresa) (int, error) {
	stmt, err := client.db.Prepare(`INSERT INTO "Adrese"("NumeAdresa", "Oras", "Judet", "Sector", "Strada", "Numar", "Bloc", "Etaj") VALUES(:1, :2, :3, :4, :5, :6, :7, :8)`)
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
	rows, err := client.db.Query(`SELECT NVL(MAX("IdAdresa"), 0) FROM "Adrese"`)
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
		`
			SELECT "IdIntrare", "CodPartener", "Status", "Data", "DataLivrare", "Total", "Vat", "Discount", "Moneda", "Platit", NVL("Comentarii", 'N/A'), "CodVanzator", "IdSucursala" 
			FROM "Vanzari" 
			ORDER BY "IdIntrare"
			OFFSET 0 ROWS FETCH NEXT 15 ROWS ONLY
		`,
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
	stmt, err := client.db.Prepare(`INSERT INTO "Vanzari"("CodPartener", "Status", "Data", "DataLivrare", "Total", "Vat", "Discount", "Moneda", "Platit", "Comentarii", "CodVanzator", "IdSucursala") VALUES(:1, :2, TO_DATE(:3, 'MM/DD/YYYY'), TO_DATE(:4, 'MM/DD/YYYY'), :5, :6, :7, :8, :9, :10, :11, :12)`)
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
	rows, err := client.db.Query(`SELECT NVL(MAX("IdIntrare"), 0) FROM "Vanzari"`)
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
		`SELECT "IdIntrare", "NumarLinie", "CodArticol", "Cantitate", "Pret", "Discount", "Vat", "TotalLinie", "IdProiect" FROM "LiniiVanzari" WHERE "IdIntrare" = :1`,
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
		`SELECT NVL(MAX("NumarLinie"), 0) FROM "LiniiVanzari" WHERE "IdIntrare" = :1`,
		linie.IDIntrare,
	)
	if err != nil {
		return err
	}
	defer rows.Close()
	rows.Next()
	err = rows.Scan(&nrLinieVanzare)

	stmt, err := client.db.Prepare(`INSERT INTO "LiniiVanzari"("IdIntrare", "NumarLinie", "CodArticol", "Cantitate", "Pret", "Discount", "Vat", "TotalLinie", "IdProiect") VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9)`)
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
	stmt, err := client.db.Prepare(`UPDATE "LiniiVanzari" SET "CodArticol" = :1, "Cantitate" = :2, "Pret" = :3, "Discount" = :4, "Vat" = :5, "TotalLinie" = :6, "IdProiect" = :7 WHERE "IdIntrare" = :8 AND "NumarLinie" = :9`)
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
	stmt, err := client.db.Prepare(`DELETE FROM "LiniiVanzari" WHERE "IdIntrare" = :1 AND "NumarLinie" = :2`)
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
		`SELECT "CodArticol", "NumeArticol", "CodGrupa", "CantitateStoc", "IdUnitateDeMasura" FROM "Articole"`,
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
	stmt, err := client.db.Prepare(`INSERT INTO "Articole"("CodArticol", "NumeArticol", "CodGrupa", "CantitateStoc", "IdUnitateDeMasura") VALUES(:1, :2, :3, :4, :5)`)
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

	rows, err := client.db.Query(
		`SELECT "CodVanzator", "Nume", "Prenume", "SalariuBaza", "Comision", "EMail", "IdAdresa" FROM "Vanzatori"`,
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

	return vanzatori, nil
}

func (client DBClient) InsertVanzator(vanzatorAdresa repositories.InsertVanzator) error {
	IDAdresa, err := client.InsertAdresa(vanzatorAdresa.Adresa)
	if err != nil {
		return err
	}

	vanzator := vanzatorAdresa.Vanzator
	stmt, err := client.db.Prepare(`INSERT INTO "Vanzatori"("Nume", "Prenume", "SalariuBaza", "Comision", "EMail", "IdAdresa") VALUES(:1, :2, :3, :4, :5, :6)`)
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
		`SELECT "IdSucursala", "NumeSucursala", "IdAdresa" FROM "Sucursale"`,
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

func (client DBClient) InsertSucursala(sucursalaAdresa repositories.InsertSucursala) error {
	IDAdresa, err := client.InsertAdresa(sucursalaAdresa.Adresa)
	if err != nil {
		return err
	}

	sucursala := sucursalaAdresa.Sucursala
	stmt, err := client.db.Prepare(`INSERT INTO "Sucursale"("NumeSucursala", "IdAdresa") VALUES(:1, :2)`)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(
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
		`SELECT "IdProiect", "NumeProiect", "ValidDeLa", "ValidPanaLa", "Activ" FROM "Proiecte"`,
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
	stmt, err := client.db.Prepare(`INSERT INTO "Proiecte"("IdProiect", "NumeProiect", "ValidDeLa", "ValidPanaLa", "Activ") VALUES(:1, :2, TO_DATE(:3, 'MM/DD/YYYY'), TO_DATE(:4, 'MM/DD/YYYY'), :5)`)
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
		`SELECT "CodGrupa", "NumeGrupa", NVL("DetaliiGrupa", ' ') FROM "GrupaArticole"`,
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
		`SELECT "IdUnitateDeMasura", "NumeUnitateDeMasura", "Inaltime", "Latime", "Lungime" FROM "UnitatiDeMasura"`,
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

	rows, err := client.db.Query(`
		SELECT NVL(SUM(fv."Platit"), 0) VanzareTotala, da."NumeGrupa"
		FROM "Fapt_Vanzare" fv, "Dimensiune_Articol" da
		WHERE fv."CodArticol" = da."CodArticol"
		GROUP BY da."NumeGrupa"
		
	`)
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

	rows, err := client.db.Query(`
		SELECT NVL(AVG(fv."Cantitate"), 0) CantitateMedie, da."NumeUnitateDeMasura", sda."Judet"
		FROM "Fapt_Vanzare" fv, "Dimesiune_Sucursala" ds, "SubDimensiune_Adresa" sda, "Dimensiune_Articol" da
		WHERE fv."IdSucursala" = ds."IdSucursala" AND ds."IdAdresa" = sda."IdAdresa" AND fv."CodArticol" = da."CodArticol"
		GROUP BY da."NumeUnitateDeMasura", sda."Judet"
	`)
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

	rows, err := client.db.Query(`
		SELECT NVL(AVG(fv."Dicount" * 100 / NVL(fv."Pret", 1)), 0) ProcentDiscount, dd."An" || '-' || 'q' || dd."Trimestru" Trimestru
		FROM "Fapt_Vanzare" fv, "Dimesiune_Data" dd
		WHERE fv."Data" = dd."Data"
		GROUP BY dd."An", dd."Trimestru"
		ORDER BY Trimestru
	`)
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

func (client DBClient) GetVolumLivratZile(dataStart string, dataEnd string) ([]repositories.VolumLivratZile, error) {
	var (
		results          []repositories.VolumLivratZile
		ziSaptamana      string
		volumMediuLivrat float32
	)

	whereStatement := ""
	if len(dataStart) > 0 {
		whereStatement = fmt.Sprintf("%s%s%s", `WHERE fv."Data" >= TO_DATE(`, dataStart, `, 'MM/DD/YYYY')`)
	}
	if len(dataEnd) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = fmt.Sprintf("%s%s%s", `WHERE fv."Data" <= TO_DATE(`, dataEnd, `, 'MM/DD/YYYY')`)
		} else {
			whereStatement = fmt.Sprintf("%s%s%s", ` AND fv."Data" <= TO_DATE(`, dataEnd, `, 'MM/DD/YYYY')`)
		}
	}

	rows, err := client.db.Query(fmt.Sprintf("%s\n%s\n%s",
		`
		SELECT NVL(AVG(fv."Volum"), 0) VolumMediuLivrat, TO_CHAR(fv."DataLivrare", 'DY') ZiSaptamana
		FROM "Fapt_Vanzare" fv
		`,
		whereStatement,
		`GROUP BY TO_CHAR(fv."DataLivrare", 'DY')`,
	))
	if err != nil {
		return []repositories.VolumLivratZile{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&volumMediuLivrat, &ziSaptamana)
		if err != nil {
			return []repositories.VolumLivratZile{}, err
		}

		results = append(
			results,
			repositories.VolumLivratZile{
				ZiSaptamana:      ziSaptamana,
				VolumMediuLivrat: volumMediuLivrat,
			},
		)
	}

	err = rows.Err()
	if err != nil {
		return []repositories.VolumLivratZile{}, err
	}

	return results, nil
}

func (client DBClient) GetFormReport(params repositories.FormParams) ([]repositories.FormResult, error) {
	selectStatement := `SELECT fv."Pret", fv."Cantitate", fv."Vat", fv."Dicount", fv."Platit", fv."Comision", fv."Volum", fv."NumarTranzactii"`
	fromStatement := `FROM "Fapt_Vanzare" fv`

	query := getReportQueryBasedOnFormParams(selectStatement, fromStatement, params)
	fmt.Println(query)

	var (
		results         []repositories.FormResult
		pret            float32
		cantitate       float32
		vat             float32
		discount        float32
		platit          float32
		comision        float32
		volum           float32
		numarTranzactii float32
	)

	rows, err := client.db.Query(query)
	if err != nil {
		return []repositories.FormResult{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&pret, &cantitate, &vat, &discount, &platit, &comision, &volum, &numarTranzactii)
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
				Comision:        comision,
				Volum:           volum,
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
	selectStatement := `
		SELECT NVL(SUM(fv."Pret"), 0) PretTotal, NVL(SUM(fv."Cantitate"), 0) CantitateTotal, NVL(SUM(fv."Vat"), 0) VatTotal, 
			NVL(SUM(fv."Dicount"), 0) DiscountTotal, NVL(SUM(fv."Platit"), 0) PlatitTotal, NVL(SUM(fv."Comision"), 0) ComisionTotal, 
			NVL(SUM(fv."Volum"), 0) VolumTotal, NVL(SUM(fv."NumarTranzactii"), 0) NumarTranzactiiTotal,
			NVL(AVG(fv."Pret"), 0) PretMediu, NVL(AVG(fv."Cantitate"), 0) CantitateMedie, NVL(AVG(fv."Vat"), 0) VatMediu, 
			NVL(AVG(fv."Dicount"), 0) DiscountMediu, NVL(AVG(fv."Platit"), 0) PlatitMedie, NVL(AVG(fv."Comision"), 0) ComisionMediu, 
			NVL(AVG(fv."Volum"), 0) VolumMediu, NVL(AVG(fv."NumarTranzactii"), 0) NumarTranzactiiMediu 
	`
	fromStatement := `FROM "Fapt_Vanzare" fv`

	query := getReportQueryBasedOnFormParams(selectStatement, fromStatement, params)
	fmt.Println(query)

	var (
		results              []repositories.FormResult
		pretTotal            float32
		cantitateTotal       float32
		vatTotal             float32
		discountTotal        float32
		platitTotal          float32
		comisionTotal        float32
		volumTotal           float32
		numarTranzactiiTotal float32
		pretMediu            float32
		cantitateMedie       float32
		vatMediu             float32
		discountMediu        float32
		platitMedie          float32
		comisionMediu        float32
		volumMediu           float32
		numarTranzactiiMediu float32
	)

	rows, err := client.db.Query(query)
	if err != nil {
		return []repositories.FormResult{}, err
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&pretTotal, &cantitateTotal, &vatTotal, &discountTotal, &platitTotal, &comisionTotal, &volumTotal, &numarTranzactiiTotal,
			&pretMediu, &cantitateMedie, &vatMediu, &discountMediu, &platitMedie, &comisionMediu, &volumMediu, &numarTranzactiiMediu)
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
				Comision:        comisionTotal,
				Volum:           volumTotal,
				NumarTranzactii: numarTranzactiiTotal,
			},
			repositories.FormResult{
				Pret:            pretMediu,
				Cantitate:       cantitateMedie,
				Vat:             vatMediu,
				Discount:        discountMediu,
				Platit:          platitMedie,
				Comision:        comisionMediu,
				Volum:           volumMediu,
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

func getReportQueryBasedOnFormParams(selectStatement string, fromStatement string, params repositories.FormParams) string {
	whereStatement := ""

	if params.CodVanzator != 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s %d", whereStatement, `fv."CodVanzator" =`, params.CodVanzator)
	}

	if len(params.NumeArticol) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s '%s'", whereStatement, `fv."CodArticol" = da."CodArticol" AND da."NumeArticol" =`, params.NumeArticol)
		fromStatement = fmt.Sprintf(`%s, "Dimensiune_Articol" da`, fromStatement)
	}

	if len(params.NumeSucursala) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s '%s'", whereStatement, `fv."IdSucursala" = ds."IdSucursala" AND ds."NumeSucursala" =`, params.NumeSucursala)
		fromStatement = fmt.Sprintf(`%s, "Dimesiune_Sucursala" ds`, fromStatement)
	}

	if len(params.NumePartener) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s '%s'", whereStatement, `fv."CodPartener" = dp."CodPartener" AND dp."NumePartener" =`, params.NumePartener)
		fromStatement = fmt.Sprintf(`%s, "Dimensiune_Partener" dp`, fromStatement)
	}

	if len(params.DataStart) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s'%s'%s", whereStatement, `fv."Data" >= TO_DATE(`, params.DataStart, `, 'MM/DD/YYYY')`)
	}

	if len(params.DataEnd) > 0 {
		if len(whereStatement) == 0 {
			whereStatement = "WHERE "
		} else {
			whereStatement = fmt.Sprintf("%s AND ", whereStatement)
		}
		whereStatement = fmt.Sprintf("%s %s'%s'%s", whereStatement, `fv."Data" <= TO_DATE(`, params.DataEnd, `, 'MM/DD/YYYY')`)
	}

	return fmt.Sprintf("%s\n%s\n%s", selectStatement, fromStatement, whereStatement)
}
