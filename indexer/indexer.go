package main

import (
	//"bytes"
	//"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"runtime/pprof"
	"sync"
)

// Estrucutra de datos para indexar un correo
type MailObj struct {
	Message_ID          string
	Date                time.Time
	From                string
	To                  string
	Subject             string
	Body				string
	Folder				string

}

// endpoint zincSearch para ingregar los datos
const URL = "http://localhost:5080/api/default/enron2/_multi"

// Credenciales zincSearch
const USER = "root@example.com"
const PWD = "Complexpass#123"
var authEncoded string


//Directorios a analizar
var ENRON_MAIL string
var MAIL_DIR string

//Formato de fecha
// De acuerdo a la documentacion se debe usar esta fecha especifiamente para darle formato a la fecha
const DATE_LAYOUT = "Mon, 2 Jan 2006 15:04:05 -0700 (MST)"

//Para administrar las rutinas
var waitGroup sync.WaitGroup

func main(){
	if (len(os.Args)<= 1){
		log.Fatal("Mails path has not been supplied as an argument")
	}
	// Credenciales zincsearch
	authEncoded = base64.StdEncoding.EncodeToString([]byte(USER+":"+PWD))

	//Ruta carpeta a indexar
	ENRON_MAIL = os.Args[1]
	MAIL_DIR = ENRON_MAIL + "\\maildir"

	//Profilling del programa, Se inicia la acción de perfilación en la parte critica del programa
	profillingFile, err := os.Create("cpu.prof")
    handleError(err)
    defer profillingFile.Close()
	err = pprof.StartCPUProfile(profillingFile);
    handleError(err)
    defer pprof.StopCPUProfile()

	// Ciclo en todas las carpetas dentro de la
	folders, err := os.ReadDir(MAIL_DIR)
	handleError(err)
	indexAll(folders) //Se indexan los datos de todas las carpetas

	waitGroup.Wait()
	fmt.Println("Todas las operaciones terminadas con exito")
}

func indexAll(folders []fs.DirEntry){
	for _,folder := range folders{
		waitGroup.Add(1)
		go folderRoutine(folder.Name()) // Se llama a una rutina por cada carpeta para que haga cada carpeta en una gorutine
	}
}

func folderRoutine(folderName string){
	var startTime time.Time
	var endTime time.Time
	// para marcar que la rutina ha terminado cuando termine la ejecución de la función
	defer waitGroup.Done()

	person := folderName
	//Se la variable que se usará para ingresar la informacion
	var bulkJson strings.Builder

	//bulkJson.Grow(100000000)

	// Se recorren los archhivos de cada carpeta
	startTime = time.Now()
	err := filepath.WalkDir(filepath.Join(MAIL_DIR,person), func (path string, d fs.DirEntry, err error) error{
		handleError(err)
		// ya que la primera entrada de una funcion WalkDir es la ruta misma
		// Se asegura que no se analice a si mismo si el directorio que lo contiene es maildir
		if (filepath.Dir(path) == MAIL_DIR){
			return nil
		}
		// Se determina si la entrada d es un archivo o una carpeta
		if (!d.IsDir()){
			// Se lee el archivo y Se pasa a string 
			mailFile,_err := os.Open(path)
			handleError(_err)
			defer mailFile.Close()
			
			
			addMailToJson(mailFile, &bulkJson)
			
		}
		return nil
	})
	endTime = time.Now()
	handleError(err)
	
	
	// Se crea la petición y como cuerpo de la petición el bulkJson
	request, err := http.NewRequest("POST", URL, strings.NewReader(bulkJson.String()))
	handleError(err)
	request.Header.Set("Authorization","Basic "+ authEncoded)
	request.Header.Set("Content-Type", "application/json")

	// Se instancia el cliente y se hace la petición a través del cliente
	client := &http.Client{}
	response, err := client.Do(request)
	handleError(err)
	defer response.Body.Close()

	// Se muestra la respuesta
	body,_:= io.ReadAll(response.Body)
	
	fmt.Println(string(body)+" "+folderName+" time: "+endTime.Sub(startTime).String())
}

func addMailToJson (mailFile *os.File, bulkJson *strings.Builder) {
	var mailObj MailObj
	mailObj.From = ""
	mailObj.To = ""
	mailObj.Subject = ""
	

	content, err := io.ReadAll(mailFile)
	handleError(err)

	mailString := string(content)
	mailParts := strings.SplitN(mailString, "\r\n\r\n", 2)

	header := mailParts[0]
	body := mailParts[1]

	headerLines := strings.SplitN(header, "\r\n", -1)

	// Comodines para clave valor de las lineas del header
	var key string
	var value string

	headerLoop:
	for _, line := range headerLines {
		
		lineSplited := strings.SplitN(line, ": ", 2)
		
		if (!strings.HasPrefix(lineSplited[0], "\t") && !strings.HasPrefix(lineSplited[0], " ") ) {
			key = lineSplited[0]
			value = lineSplited[1]
		}else{
			value = lineSplited[0]
		}

		
        // Se asigna cada atributo respectivamente
		switch(key){
			case "Message-ID":
				mailObj.Message_ID = value
			case "Date":
				mailObj.Date,_ = time.Parse(DATE_LAYOUT,value)
			case "From":
				mailObj.From += value
			case "To":
				mailObj.To += value
			case "Subject":
				mailObj.Subject += value
			case "Mime-Version":
				break headerLoop
		}
	}

	mailObj.To = strings.ReplaceAll(mailObj.To,"\t","")
	mailObj.Body = body
	
	// Get the absolute path of the file
    absPath, err := filepath.Abs(mailFile.Name())
    handleError(err)
	mailObj.Folder = absPath

	// Acá se transforma la structura mailObj a un objeto json para unirlo al json del stream
	mailObjJson, err := json.Marshal(mailObj)
	handleError(err)

	//_, err = bulkJson.WriteString("{ \"index\" : { \"_index\" : \"enron\" } }\n"+string(mailObjJson)+"\n")
	_, err = bulkJson.WriteString(string(mailObjJson)+"\n")
	handleError(err)
}

func handleError (err error){
	if err!=nil{
		log.Fatal(err)
	}
}