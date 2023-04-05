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
)

type MailObj struct {
	Message_ID          string
	Date                time.Time
	From                string
	To                  []string
	Subject             string
	Body				string


}

func main(){
	if (len(os.Args)<= 1){
		log.Fatal("Mails path has not been supplied as an argument")
	}

	// Credenciales zincsearch
	const user = "donovan57ra@gmail.com"
	const password = "donovan#123"
	authEncoded := base64.StdEncoding.EncodeToString([]byte(user+":"+password))

	// endpoint zincobserve
	const url = "http://localhost:5080/api/default/_bulk"

	//Ruta carpeta a indexar
	enronMail := os.Args[1]
	mailDir := enronMail + "\\maildir"

	// cadena que se usará para enviar el cuerpo de la petición para indexar la data
	var bulkJson string

	// Se define la funcion para indexar la carpeta de una persona
	indexPersonFolder := func (path string, d fs.DirEntry, err error) error{
		if (err != nil){
			log.Fatal(err)
		}
	
		// ya que la primera entrada de una funcion WalkDir es la ruta misma
		// Se asegura que no se analice a si mismo si el directorio que lo contiene es maildir
		if (filepath.Dir(path) == mailDir){
			return nil
		}
		//Se va a usar un archivo json con un listado de todos los mails como objetos
		//Luego se enviará como un stream a zincSearch usando el endpoint http://localhost:5080/api/{org_id}/{stream_name}/_json
	
		
	
		// Se determina si la entrada d es un archivo o una carpeta
		if (!d.IsDir()){
			// Se lee el archivo y Se pasa a string 
			mailFile := readFileAsString(path)
			// Se agrega la informacion al json
			addMailToJson(mailFile, &bulkJson)
			return nil
		}
		
		
		
		// returns a slice for subDirs and a slice for files 
		return nil
	}


	//Profilling del programa, Se inicia la acción de perfilación en la parte critica del programa
	profillingFile, err := os.Create("cpu.prof")
    if err != nil {
        log.Fatal(err)
    }
    defer profillingFile.Close()
	err = pprof.StartCPUProfile(profillingFile);
    if  err != nil {
        log.Fatal(err)
    }
    defer pprof.StopCPUProfile()


	// Ciclo en todas las carpetas dentro de la
	folders, err := os.ReadDir(mailDir)
	if err!=nil{
		log.Fatal(err)
	}

	for _,folder := range folders{
		person := folder.Name()
		bulkJson = ""
		// Se recorren los archhivos de cada carpeta
		err = filepath.WalkDir(filepath.Join(mailDir,person), indexPersonFolder)
		if err != nil{
			log.Fatal(err)
		}

		// Se crea la petición
		request, err := http.NewRequest("POST", url, strings.NewReader(bulkJson))
		if err != nil {
			log.Fatal(err)
		}
		request.Header.Set("Authorization","Basic "+ authEncoded)
		request.Header.Set("Content-Type", "application/json")

		// Se instancia el cliente y se hace la petición a través del cliente
		client := &http.Client{}
		response, err := client.Do(request)
		if err != nil {
			log.Fatal(err)
		}
		defer response.Body.Close()

		// Se muestra la respuesta
		body,_:= io.ReadAll(response.Body)
		fmt.Println(string(body))
	}
}




func addMailToJson (mailFile string, bulkJson *string) {
	// Logica para agregar la informacion del mail al json
	
	// Linea en la que acaba el header
	headerEnd := 0

	//Se crea una estructura de datos para el mail
	var mailObj MailObj
	// Se crea un scanner para leer el string linea por linea
	scanner := bufio.NewScanner(strings.NewReader(mailFile))
	// Se incrementa el tamaño del buffer de cada linea
	scanner.Buffer(make([]byte, 0, 64*1024),1024*1024) 
    lineNumber := 1 
    headerLoop : for scanner.Scan() { // Se pueden poner etiquetas en el codigo que buen detalle
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
				mailObj.To = strings.Split(strings.ReplaceAll(line[1]," ",""), ",")
			case "Subject":
				mailObj.Subject = line[1]
			case "X-FileName": // En el caso de este atributo se sabe que es el ultimo del header y por lo tanto la ultima linea del mismo
				headerEnd = lineNumber
				break headerLoop // Se hace break al loop marcado con la etiqueta
		}
        lineNumber++
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

	// Para leer el body 
	body := ""
	lineNumber = 1
	scanner = bufio.NewScanner(strings.NewReader(mailFile))
	scanner.Buffer(make([]byte, 0, 64*1024),1024*1024) 
    for scanner.Scan() {
		if (lineNumber > headerEnd){
        	body += scanner.Text() + "\n"
		}
		lineNumber ++
    }
    if err := scanner.Err(); err != nil {
        log.Fatal(err)
    }

	mailObj.Body = body

	// Acá se transforma la structura mailObj a un objeto json para unirlo al json del stream
	mailObjJson, err := json.Marshal(mailObj)
	if (err != nil){
		log.Fatal(err)
	}


	*bulkJson += "{ \"index\" : { \"_index\" : \"enron\" } }\n"+string(mailObjJson)+"\n"

}

func readFileAsString (fpath string) string {
	fileBytes, _err := os.ReadFile(fpath)
	if _err != nil{
		fmt.Printf("No se pudo leer el archivo %s\n", filepath.Base(fpath))
		log.Fatal(_err)
	}
	// Se pasa a string 
	return string(fileBytes)
}