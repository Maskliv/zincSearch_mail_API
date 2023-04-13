package main

import (
	"bufio"
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
const URL = "http://localhost:5080/api/default/_bulk"

// Credenciales zincSearch
const USER = "donovan57ra@gmail.com"
const PWD = "donovan#123"
var authEncoded string


//Directorios a analizar
var ENRON_MAIL string
var MAIL_DIR string

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
	// para marcar que la rutina ha terminado cuando termine la ejecución de la función
	defer waitGroup.Done()

	person := folderName
	//Se la variable que se usará para ingresar la informacion
	bulkJson := ""

	// Se recorren los archhivos de cada carpeta
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
	handleError(err)
	
	// Se crea la petición y como cuerpo de la petición el bulkJson
	request, err := http.NewRequest("POST", URL, strings.NewReader(bulkJson))
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
	fmt.Println(string(body)+" "+folderName)
}

func addMailToJson (mailFile *os.File, bulkJson *string) {
	// Logica para agregar la informacion del mail al json
	// Linea en la que acaba el header
	headerEnd := false
	//Se crea una estructura de datos para el mail
	var mailObj MailObj
	mailObj.To = ""
	// Se crea un scanner para leer el string linea por linea
	scanner := bufio.NewScanner(mailFile)
	// Se incrementa el tamaño del buffer de cada linea
	scanner.Buffer(make([]byte, 0, 64*1024),1024*1024) 

	lineNumber := 1 
	body:=""
    for scanner.Scan() { // Se pueden poner etiquetas en el codigo que buen detalle
		if (headerEnd){
			body += scanner.Text() + "\n"
			continue
		}

        line := strings.Split(scanner.Text(), ": ")
        // Se asigna cada atributo respectivamente
		switch(line[0]){
			case "Message-ID":
				mailObj.Message_ID = line[1]
			case "Date":
				// De acuerdo a la documentacion se debe usar esta fecha especifiamente para darle formato a la fecha
				layout := "Mon, 2 Jan 2006 15:04:05 -0700 (MST)"
				mailObj.Date,_ = time.Parse(layout,line[1])
			case "From":
				mailObj.From = line[1]
			case "To":
				mailObj.To += line[1]
			case "Subject":
				mailObj.Subject = line[1]
			case "X-FileName": // En el caso de este atributo se sabe que es el ultimo del header y por lo tanto la ultima linea del mismo
				headerEnd = true
		}
		if strings.Contains(line[0], "@"){
			mailObj.To += line[0]
		}
        lineNumber++
    }

	if err:=scanner.Err(); err!=nil{
		log.Fatal(err)
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

	*bulkJson += "{ \"index\" : { \"_index\" : \"enron\" } }\n"+string(mailObjJson)+"\n"
}

func handleError (err error){
	if err!=nil{
		log.Fatal(err)
	}
}