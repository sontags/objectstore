package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/unprofession-al/objectstore"
)

type App struct {
	object string
}

func main() {
	var app App
	flag.StringVar(&app.object, "o", "", "object path to test with")
	flag.Parse()

	o, err := objectstore.New(app.object)
	exitOnErr(err)

	testData := []byte("Hello, World!")

	err = o.Write(testData)
	exitOnErr(err)
	fmt.Printf("\nData written at '%s', press the enter to continue...", app.object)
	fmt.Scanln()

	data, err := o.Read()
	exitOnErr(err)
	fmt.Printf("\nData read from '%s', press the enter to continue...", app.object)
	fmt.Scanln()

	if res := bytes.Compare(data, testData); res != 0 {
		err = fmt.Errorf("The data stored does not match the data read")
		exitOnErr(err)
	}
	fmt.Printf("\nData compared, press the enter to continue...")
	fmt.Scanln()

	err = o.Delete()
	exitOnErr(err)
	fmt.Printf("\nObject '%s' deleted, press the enter to continue...", app.object)
	fmt.Scanln()
}

func exitOnErr(errs ...error) {
	errNotNil := false
	for _, err := range errs {
		if err == nil {
			continue
		}
		errNotNil = true
		fmt.Fprintf(os.Stderr, "ERROR: %s", err.Error())
	}
	if errNotNil {
		fmt.Print("\n")
		os.Exit(-1)
	}
}
