package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/unprofession-al/objectstore"
)

type App struct {
	path   string
	object string
}

func main() {
	var app App
	flag.StringVar(&app.path, "p", "", "path to test with")
	flag.StringVar(&app.object, "o", "", "object name to test with")
	flag.Parse()

	fqon := fmt.Sprintf("%s/%s", app.path, app.object)
	fmt.Printf("\nTesting with '%s', press the enter to continue...", fqon)
	fmt.Scanln()

	o, err := objectstore.New(app.path)
	exitOnErr(err)

	testData := []byte("Hello, World!")

	err = o.Write(app.object, testData)
	exitOnErr(err)
	fmt.Printf("\nData written at '%s', press the enter to continue...", fqon)
	fmt.Scanln()

	data, err := o.Read(app.object)
	exitOnErr(err)
	fmt.Printf("\nData read from '%s', press the enter to continue...", fqon)
	fmt.Scanln()

	if res := bytes.Compare(data, testData); res != 0 {
		err = fmt.Errorf("the data stored does not match the data read")
		exitOnErr(err)
	}
	fmt.Printf("\nData compared, press the enter to continue...")
	fmt.Scanln()

	list, err := o.List()
	exitOnErr(err)
	found := false
	for _, on := range list {
		if on == app.object {
			found = true
		}
	}
	if !found {
		err = fmt.Errorf("the object stored was not found while listing the base")
		exitOnErr(err)
	}
	fmt.Printf("\nData listed, press the enter to continue...")
	fmt.Scanln()

	err = o.Delete(app.object)
	exitOnErr(err)
	fmt.Printf("\nObject '%s' deleted, press the enter to continue...", fqon)
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
