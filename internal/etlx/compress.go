package etlxlib

import (
	"archive/zip"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
)

func (etlx *ETLX) CompressToZip(files []string, output string) error {
	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()
	zipWriter := zip.NewWriter(outFile)
	defer zipWriter.Close()
	for _, file := range files {
		inFile, err := os.Open(file)
		if err != nil {
			return err
		}
		defer inFile.Close()

		w, err := zipWriter.Create(filepath.Base(file))
		if err != nil {
			return err
		}
		_, err = io.Copy(w, inFile)
		if err != nil {
			return err
		}
	}
	return nil
}

func (etlx *ETLX) CompressToGZ(input string, output string) error {
	inFile, err := os.Open(input)
	if err != nil {
		return err
	}
	defer inFile.Close()
	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()
	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()
	_, err = io.Copy(gzWriter, inFile)
	return err
}
