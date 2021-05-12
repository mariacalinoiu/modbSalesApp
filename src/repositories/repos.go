package repositories

type (
	Partener struct {
		CodPartener  string `json:"CodPartener"`
		NumePartener string `json:"NumePartener"`
		CUI          string `json:"CUI"`
		Email        string `json:"Email"`
		IDAdresa     int    `json:"IDAdresa"`
	}

	Vanzare struct {
		IDIntrare   int     `json:"IDIntrare"`
		CodPartener string  `json:"CodPartener"`
		Status      string  `json:"Status"`
		Data        string  `json:"Data"`
		DataLivrare string  `json:"DataLivrare"`
		Total       float32 `json:"Total"`
		VAT         float32 `json:"VAT"`
		Discount    float32 `json:"Discount"`
		Moneda      string  `json:"Moneda"`
		Platit      float32 `json:"Platit"`
		Comentarii  string  `json:"Comentarii"`
		CodVanzator int     `json:"CodVanzator"`
		IDSucursala int     `json:"IDSucursala"`
	}

	LinieVanzare struct {
		IDIntrare  int     `json:"IDIntrare"`
		NumarLinie int     `json:"NumarLinie"`
		CodArticol string  `json:"CodArticol"`
		Cantitate  float32 `json:"Cantitate"`
		Pret       float32 `json:"Pret"`
		Discount   float32 `json:"Discount"`
		VAT        float32 `json:"VAT"`
		TotalLinie float32 `json:"TotalLinie"`
		IDProiect  string  `json:"IDProiect"`
	}

	Proiect struct {
		IDProiect   string `json:"IDProiect"`
		NumeProiect string `json:"NumeProiect"`
		ValidDeLa   string `json:"ValidDeLa"`
		ValidPanaLa string `json:"ValidPanaLa"`
		Activ       string `json:"Activ"`
	}

	Articol struct {
		CodArticol      string `json:"CodArticol"`
		NumeArticol     string `json:"NumeArticol"`
		CodGrupa        int    `json:"CodGrupa"`
		CantitateStoc   int    `json:"CantitateStoc"`
		IDUnitateMasura int    `json:"IDUnitateMasura"`
	}

	GrupaArticole struct {
		CodGrupa     int    `json:"CodGrupa"`
		NumeGrupa    string `json:"NumeGrupa"`
		DetaliiGrupa string `json:"DetaliiGrupa"`
	}

	UnitateDeMasura struct {
		IDUnitateMasura     int     `json:"IDUnitateMasura"`
		NumeUnitateDeMasura string  `json:"NumeUnitateDeMasura"`
		Inaltime            float32 `json:"Inaltime"`
		Latime              float32 `json:"Latime"`
		Lungime             float32 `json:"Lungime"`
	}

	Sucursala struct {
		IDSucursala   int    `json:"IDSucursala"`
		NumeSucursala string `json:"NumeSucursala"`
		IDAdresa      int    `json:"IDAdresa"`
	}

	Vanzator struct {
		CodVanzator int     `json:"CodVanzator"`
		Nume        string  `json:"Nume"`
		Prenume     string  `json:"Prenume"`
		SalariuBaza float32 `json:"SalariuBaza"`
		Comision    float32 `json:"Comision"`
		Email       string  `json:"Email"`
		IDAdresa    int     `json:"IDAdresa"`
	}

	Adresa struct {
		IDAdresa   int    `json:"IDAdresa"`
		NumeAdresa string `json:"NumeAdresa"`
		Oras       string `json:"Oras"`
		Judet      string `json:"Judet"`
		Sector     string `json:"Sector"`
		Strada     string `json:"Strada"`
		Numar      string `json:"Numar"`
		Bloc       string `json:"Bloc"`
		Etaj       int    `json:"Etaj"`
	}

	InsertPartener struct {
		Partener Partener `json:"Partener"`
		Adresa   Adresa   `json:"Adresa"`
	}

	InsertVanzator struct {
		Vanzator Vanzator `json:"Vanzator"`
		Adresa   Adresa   `json:"Adresa"`
	}

	InsertSucursala struct {
		Sucursala Sucursala `json:"Sucursala"`
		Adresa    Adresa    `json:"Adresa"`
	}

	InsertVanzare struct {
		Vanzare      Vanzare        `json:"Vanzare"`
		LiniiVanzari []LinieVanzare `json:"LiniiVanzare"`
	}

	WasSuccess struct {
		Success bool `json:"success"`
	}

	FormParams struct {
		CodVanzator   int
		NumeArticol   string
		NumePartener  string
		NumeSucursala string
		DataStart     string
		DataEnd       string
	}

	FormResult struct {
		Pret            float32 `json:"Pret"`
		Cantitate       float32 `json:"Cantitate"`
		Vat             float32 `json:"VAT"`
		Discount        float32 `json:"Discount"`
		Platit          float32 `json:"Platit"`
		Comision        float32 `json:"Comision"`
		Volum           float32 `json:"Volum"`
		NumarTranzactii float32 `json:"NumarTranzactii"`
	}

	VanzariGrupeArticole struct {
		NumeGrupa     string  `json:"NumeGrupa"`
		VanzareTotala float32 `json:"VanzareTotala"`
	}

	CantitateJudete struct {
		Judet          string  `json:"Judet"`
		Um             string  `json:"Um"`
		CantitateMedie float32 `json:"CantitateMedie"`
	}

	ProcentDiscountTrimestru struct {
		Trimestru       string  `json:"Trimestru"`
		ProcentDiscount float32 `json:"ProcentDiscount"`
	}

	VolumLivratZile struct {
		ZiSaptamana      string  `json:"ZiSaptamana"`
		VolumMediuLivrat float32 `json:"VolumMediuLivrat"`
	}
)
