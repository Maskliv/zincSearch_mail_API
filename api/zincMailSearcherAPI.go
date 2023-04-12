package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type QueryObj struct {
	EndTime int64 `json:"end_time"`
	From int32 `json:"from"`
	Size int32 `json:"size"`
	Sql string `json:"sql"`
	SqlMode string `json:"sql_mode"`
	StartTime int64 `json:"start_time"`
	TrackTotalHits bool `json:"track_total_hits"`
}

type ResponseObj struct {
	Took  int64 `json:"took"`
	Hits []Email `json:"hits"`
	Total int64 `json:"total"`
	From int64 `json:"from"`
	Size int64 `json:"size"`
	ScanSize int64 `json:"scan_size"`
}


type Email struct {
	Timestamp int64 `json:"_timestamp"`
	Body string `json:"body"`
	Date time.Time  `json:"date"`
	From string `json:"from"`
	MessageId string `json:"message_id"`
	Subject string `json:"subject"`
	To0 string `json:"to_0_"`
	To1 string `json:"to_1_"`
	To2 string `json:"to_2_"`
	To3 string `json:"to_3_"`
	To4 string `json:"to_4_"`
	To5 string `json:"to_5_"`
}

type EmailDTO struct {
	Id          		int 
	Date                time.Time
	From                string
	To                  []string
	Subject             string
	Body				string
}

const STREAM = "enron"
// endpoint zincSearch para ingregar los datos
const URL = "http://localhost:5080/api/default/_search"

// Credenciales zincSearch
const USER = "donovan57ra@gmail.com"
const PWD = "donovan#123"
var authEncoded string


func main(){
	// Se codifica los datos de inicio de sesión
	authEncoded = base64.StdEncoding.EncodeToString([]byte(USER+":"+PWD))

	port := ""

	configurePort(&port)
	
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
		w.WriteHeader(200)
		w.Write([]byte("<h1>Go server working well!</h1>"))
	})

	r.Get("/search/{input}", func (w http.ResponseWriter, r *http.Request)  {
		// definimos la variable de resultados
		var results []EmailDTO
		//Definimos el tiempo actual en microsegundos para el parametro end_time de la consulta
		currentTimeMicroseconds := time.Now().UnixMicro()
		// Se extrae el texto ingresado por el usuario y que desea buscar
		searchInput := chi.URLParam(r, "input");
		// Se define el objeto query mediante el cual se realiza la consulta
		var query QueryObj
		query.EndTime = currentTimeMicroseconds
		query.From = 0
		query.Size = 2
		query.Sql = fmt.Sprintf("SELECT * FROM %s WHERE match_all('%s')", STREAM, searchInput) // Manera de hacer la busqueda
		query.SqlMode = "context"
		query.StartTime = 0
		query.TrackTotalHits = false
		
		queryJson, err := json.Marshal(query)
		handleError(err)
		// Se estructura el cuerpo de la petición de la consulta 
		searchBody := fmt.Sprintf("{\n\"query\": %s\n}", queryJson) 


		searchRequest, err := http.NewRequest("POST", URL, strings.NewReader(searchBody))
		handleError(err)
		searchRequest.Header.Set("Authorization","Basic "+ authEncoded)
		searchRequest.Header.Set("Content-Type", "application/json")

		// Se instancia el cliente y se hace la petición a través del cliente
		client := &http.Client{}
		response, err := client.Do(searchRequest)
		handleError(err)
		defer response.Body.Close()

		// Se muestra la respuesta
		body,_:= io.ReadAll(response.Body)

		var responseObj ResponseObj
		json.Unmarshal(body, &responseObj)

		hitsToEmailDTO(responseObj.Hits, &results)
		//fmt.Println(results)
		resultsJson,_ := json.Marshal(results)

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
		w.WriteHeader(200)
		
		w.Write(resultsJson)

	})

	http.ListenAndServe(":"+port, r)
}

func configurePort(port *string){
	for i, arg := range os.Args {
		if (arg == "-port" && len(os.Args)>=(i+1) ){
			*port = os.Args[i+1]
		}
		
	}
	if (*port == "") {
		fmt.Println("No port has been selected, using port 3000 by default")
		*port = "3000"
	}
}



func handleError (err error){
	if err!=nil{
		log.Fatal(err)
	}
}




func hitsToEmailDTO(hits []Email, results *[]EmailDTO){
	for i,hit := range hits {
		var result EmailDTO
		result.Id = i
		result.Body = hit.Body
		result.Date = hit.Date
		result.From = hit.From
		result.Subject = hit.Subject
		if (hit.To0 != "") {result.To = append(result.To, hit.To0)}
		if (hit.To1 != "") {result.To = append(result.To, hit.To1)}
		if (hit.To2 != "") {result.To = append(result.To, hit.To2)}
		if (hit.To3 != "") {result.To = append(result.To, hit.To3)}
		if (hit.To4 != "") {result.To = append(result.To, hit.To4)}
		if (hit.To5 != "") {result.To = append(result.To, hit.To5)}

		*results = append(*results, result)
	}
}