package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
)



func main(){
	if (len(os.Args)<= 1){
		log.Fatal("Mails path has not been supplied as an argument")
	}
	enronMail := os.Args[1]
	mailDir := enronMail + "\\maildir"

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
	
		var json string
	
		// Se determina si la entrada d es un archivo o una carpeta
		if (!d.IsDir()){
			// Se lee el archivo y Se pasa a string 
			mailFile := readFileAsString(path)
			// Se agrega la informacion al json
			addMailToJson(mailFile, &json)
			return nil
		}
	
		// Se tiene que hacer otro walk sobre la carpeta
		_err := filepath.WalkDir(path, func(subPath string, subD fs.DirEntry, err error)error{
			if (err != nil){
				log.Fatal(err)
			}
			//De igual manera que arriba se ignora la propia carpeta
			if (subPath == path){
				return nil
			}

			// Se lee el archivo y Se pasa a string 
			mailFile := readFileAsString(subPath)
			// Se agrega la informacion al json
			addMailToJson(mailFile, &json)
			// Ya acá se vuelve a aplicar la misma logica de leer cada archivo y agregarlo 
			return nil
		})
	
		if _err != nil {
			fmt.Printf("Ocurrió un error recorriendo la carpeta %s\n",path)
			log.Fatal(_err)
		}
	
	
		// returns a slice for subDirs and a slice for files 
		return nil
	}


	err := filepath.WalkDir(filepath.Join(mailDir,"allen-p"), indexPersonFolder)
	if err != nil{
		log.Fatal(err)
	}
	
	fmt.Println("End")
}




func addMailToJson (mailFile string, json *string) {
	// Logica para agregar la informacion del mail al json
	
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