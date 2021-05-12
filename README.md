# MODB SalesApp

Acest REST API constituie nivelul intermediar care separa [frontend-ul aplicatiei](https://github.com/andreeacalin30/modb-sales-app) de baza de date. Endpoint-urile suportate sunt detaliate mai jos.

## Compilare si Executare

Mutarea in directorul proiectului: ```cd modbSalesApp/```

Executarea build-ului: ```go build src/server.go```

Pornirea serverului: ```./server```

## Endpoint-uri

/grupeArticole
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/grupeArticole
    returneaza:     un JSON care contine o lista de grupe de articole

/um
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/um
    returneaza:     un JSON care contine o lista de unitati de masura

/articole
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/articole
    returneaza:     un JSON care contine o lista de articole
    
    metoda:         POST
    exemplu URL:    http://localhost:8081/articole
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "CodArticol": "codTest",
                        "NumeArticol": "articol test",
                        "CodGrupa": 1,
                        "CantitateStoc": 5,
                        "IDUnitateMasura": 1
                    }

/parteneri
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/parteneri
    returneaza:     un JSON care contine o lista de parteneri
    
    metoda:         POST
    exemplu URL:    http://localhost:8081/parteneri
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "Partener": {
                            "CodPartener": "codtest",
                            "NumePartener": "partener test",
                            "CUI": "123456789",
                            "Email": "te@s.t"
                        },
                        "Adresa": {
                            "NumeAdresa": "a",
                            "Oras": "b",
                            "Judet": "c",
                            "Sector": "d",
                            "Strada": "e",
                            "Numar": "f",
                            "Bloc": "g",
                            "Etaj": 1
                        }
                    }

/vanzatori
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/vanzatori
    returneaza:     un JSON care contine o lista de vanzatori
    
    metoda:         POST
    exemplu URL:    http://localhost:8081/vanzatori
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "Vanzator": {
                            "Nume": "nume",
                            "Prenume": "prenume",
                            "SalariuBaza": 5221.54,
                            "Comision": 102.34,
                            "Email": "te@s.t"
                        },
                        "Adresa": {
                            "NumeAdresa": "a",
                            "Oras": "b",
                            "Judet": "c",
                            "Sector": "d",
                            "Strada": "e",
                            "Numar": "f",
                            "Bloc": "g",
                            "Etaj": 1
                        }
                    }

/vanzari
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/vanzari
    returneaza:     un JSON care contine o lista de vanzari
    
    metoda:         POST
    exemplu URL:    http://localhost:8081/vanzari
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "Vanzare": {
                            "CodPartener": "codPartener",
                            "Status": "Y",
                            "Data": "01/01/2021",
                            "DataLivrare": "01/11/2021",
                            "Total": 5112.45,
                            "VAT": 87.45,
                            "Discount": 12.45,
                            "Moneda": "ron",
                            "Platit": 6200.45,
                            "Comentarii": "comentariu test",
                            "CodVanzator": 1,
                            "IDSucursala": 1
                        },
                        "LiniiVanzare": [
                            {
                                "CodArticol": "codTest",
                                "Cantitate": 5,
                                "Pret": 24.54,
                                "Discount": 4.54,
                                "VAT": 2.54,
                                "TotalLinie": 104.54,
                                "IDProiect": "pr1"
                            },
                            {
                                "CodArticol": "codTest2",
                                "Cantitate": 10,
                                "Pret": 242.54,
                                "Discount": 24.54,
                                "VAT": 22.54,
                                "TotalLinie": 2400.54,
                                "IDProiect": "pr1"
                            },
                            {
                                "CodArticol": "codTest3",
                                "Cantitate": 2,
                                "Pret": 98.54,
                                "Discount": 1.54,
                                "VAT": 2.54,
                                "TotalLinie": 100.54,
                                "IDProiect": "pr2"
                            }
                        ]
                    }
                    
/liniiVanzari
    
    metoda:         GET
    parametri:      IDIntrare   (obligatoriu)
    exemplu URL:    http://localhost:8081/liniiVanzari?IDIntrare=1000
    returneaza:     un JSON care contine lista de linii dintr-o vanzare
    
    metoda:         POST
    exemplu URL:    http://localhost:8081/liniiVanzari
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "IDIntrare": 1000,
                        "CodArticol": "articolTest",
                        "Cantitate": 1,
                        "Pret": 1.54,
                        "Discount": 0.54,
                        "VAT": 0.54,
                        "TotalLinie": 2.54,
                        "IDProiect": "pr1"
                    }
                    
    metoda:         PUT
    exemplu URL:    http://localhost:8081/liniiVanzari
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "IDIntrare": 1000,
                        "NumarLinie": 2,
                        "CodArticol": "articolTest",
                        "Cantitate": 1,
                        "Pret": 1.54,
                        "Discount": 0.54,
                        "VAT": 0.54,
                        "TotalLinie": 2.54,
                        "IDProiect": "pr1"
                    }
                    
    metoda:         DELETE
    parametri:      IDIntrare   (obligatoriu)
                    NumarLinie  (obligatoriu)
    exemplu URL:    http://localhost:8081/liniiVanzari?IDIntrare=1000&NumarLinie=5
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    
/sucursale
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/sucursale
    returneaza:     un JSON care contine o lista de sucursale
    
    metoda:         POST
    exemplu URL:    http://localhost:8081/sucursale
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "Sucursala": {
                            "NumeSucursala": "sucursala test"
                        },
                        "Adresa": {
                            "NumeAdresa": "a",
                            "Oras": "b",
                            "Judet": "c",
                            "Sector": "d",
                            "Strada": "e",
                            "Numar": "f",
                            "Bloc": "g",
                            "Etaj": 1
                        }
                    }    
    
/proiecte
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/proiecte
    returneaza:     un JSON care contine o lista de proiecte
    
    metoda:         POST
    exemplu URL:    http://localhost:8081/proiecte
    returneaza:     un JSON care indica daca tranzactia a fost realizata cu succes
    body:           {
                        "IDProiect": "pr1",
                        "NumeProiect": "proiect test",
                        "ValidDeLa": "02/01/2021",
                        "ValidPanaLa": "04/03/2021",
                        "Activ": "Y"
                    }
    
/formReport
    
    metoda:         GET
    parametri:      CodVanzator     (optional)
                    NumeArticol     (optional)
                    NumePartener    (optional)
                    NumeSucursala   (optional)
                    DataStart       (optional)
                    DataEnd         (optional)
    exemplu URL:    http://localhost:8081/formReport?CodVanzator=1&NumePartener="test"&DataStart="12/01/2020"
    returneaza:     un JSON care contine valorile brute din depozitul de date care indeplinesc 
                    conditiile furnizate prin intermediul parametrilor

/groupedFormReport
    
    metoda:         GET
    parametri:      CodVanzator     (optional)
                    NumeArticol     (optional)
                    NumePartener    (optional)
                    NumeSucursala   (optional)
                    DataStart       (optional)
                    DataEnd         (optional)
    exemplu URL:    http://localhost:8081/groupedFormReport?NumeArticol="test"&DataStart="12/01/2020"&DataEnd="12/01/2022"
    returneaza:     un JSON care contine valorile totale (sume) si medii din depozitul de date pentru datele care indeplinesc 
                    conditiile furnizate prin intermediul parametrilor

/vanzariGrupeArticole
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/vanzariGrupeArticole
    returneaza:     un JSON care contine valorile totale (sume) ale vanzarilor, raportate pentru fiecare grupa de articole

/cantitatiJudete
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/cantitatiJudete
    returneaza:     un JSON care contine valorile medii ale vanzarilor, raportate pentru fiecare judet 
                    in functie de locatiile sucursalelor in care s-a executat vanzarea
                    
/discountTrimestre
    
    metoda:         GET
    exemplu URL:    http://localhost:8081/discountTrimestre
    returneaza:     un JSON care contine procentul mediu reprezentat de discount din valoarea platita per trimestru

/volumZile
    
    metoda:         GET
    parametri:      DataStart   (optional)
                    DataEnd     (optional)
    exemplu URL:    http://localhost:8081/volumZile?DataStart="12/01/2020"&DataEnd="12/01/2022"
    returneaza:     un JSON care contine volumul mediu livrat in fiecare zi a saptamanii pentru o perioada de timp
                    determinata de datele trimise ca aprametru