package main

import (
	"os"
	"testing"
)

func DeleteAllFiles(path string, t *testing.T) {
	dir, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err.Error())
	}
	for _, d := range dir {
		err = os.RemoveAll(path + d.Name())
		if err != nil {
			t.Fatal(err.Error())
		}
	}
}
