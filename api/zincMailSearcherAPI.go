package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	//"log"
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
	To string `json:"to"`
	Folder string `json:"folder"`
}

type EmailDTO struct {
	Id          		int 
	Date                time.Time
	From                string
	To                  []string
	Subject             string
	Body				string
}

const STREAM = "enron2"
const QUERY_SIZE = 50
// endpoint zincSearch para ingregar los datos
const URL = "http://localhost:5080/api/default/_search"

// Credenciales zincSearch
const USER = "root@example.com"
const PWD = "Complexpass#123"
var authEncoded string


func main(){
	// Se codifica los datos de inicio de sesión
	authEncoded = base64.StdEncoding.EncodeToString([]byte(USER+":"+PWD))

	port := ""

	configurePort(&port)
	
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(ErrorHandler)

	//prod
	staticHandler := http.FileServer(http.Dir("dist"))
	r.Handle("/*", http.StripPrefix("/", staticHandler))

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		/*
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
		w.WriteHeader(200)
		*/
		http.ServeFile(w, r, "dist\\index.html")
		//http.ServeFile(w, r, "dist\\favicon.ico")
	})

	r.Get("/search/{input}", func (w http.ResponseWriter, r *http.Request)  {
		// definimos la variable de resultados
		var results []EmailDTO
		//Definimos el tiempo actual en microsegundos para el parametro end_time de la consulta
		currentTimeMicroseconds := time.Now().UnixMicro()
		// Se extrae el texto ingresado por el usuario y que desea buscar
		searchInput := chi.URLParam(r, "input");
		if (!isValidInput(searchInput)){
			HandleError(w, r, fmt.Errorf("input de búsqueda inválido"))
		}
		escapeInput(&searchInput)


		// Se define el objeto query mediante el cual se realiza la consulta
		var query QueryObj
		query.EndTime = currentTimeMicroseconds
		query.From = 0
		query.Size = QUERY_SIZE
		query.Sql = fmt.Sprintf("SELECT * FROM %s WHERE match_all('%s')", STREAM, searchInput) // Manera de hacer la busqueda
		query.SqlMode = "context"
		query.StartTime = 0
		query.TrackTotalHits = false
		
		queryJson, err := json.Marshal(query)
		HandleError(w, r, err)
		// Se estructura el cuerpo de la petición de la consulta 
		searchBody := fmt.Sprintf("{\n\"query\": %s\n}", queryJson) 
		

		searchRequest, err := http.NewRequest("POST", URL, strings.NewReader(searchBody))
		HandleError(w, r, err)
		searchRequest.Header.Set("Authorization","Basic "+ authEncoded)
		searchRequest.Header.Set("Content-Type", "application/json")

		// Se instancia el cliente y se hace la petición a través del cliente
		client := &http.Client{}
		response, err := client.Do(searchRequest)
		HandleError(w, r, err)
		defer response.Body.Close()

		// Se muestra la respuesta
		body,_:= io.ReadAll(response.Body)

		var responseObj ResponseObj
		json.Unmarshal(body, &responseObj)

		hitsToEmailDTO(responseObj.Hits, &results)
		//fmt.Println(results)
		resultsJson, err:= json.Marshal(results)
		HandleError(w, r, err)

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

func ErrorHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				// Devolver un error HTTP 500
				http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func HandleError(w http.ResponseWriter, r *http.Request, err error) {
	if (err != nil){
		// Registrar el error, enviar notificación, etc.
		fmt.Println(err.Error())
		// Devolver un error HTTP 500
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func isValidInput(input string) bool {
	if ( len(input) > 1024){
		return false
	}

	specialChars := []string{"!", "#", "$", "%", "&"}
    for _, c := range specialChars {
        if strings.Contains(input, c) {
            return false
        }
    }
    return true
}

func escapeInput(input *string){
	escapeChars := []string{"'", "\"", "\\"}

	escapeChar := "\\"

	for _, c := range escapeChars {
		*input = strings.ReplaceAll(*input, c, escapeChar+c)
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
		result.To = strings.Split(hit.To, ", ")

		*results = append(*results, result)
	}
}