package etlxlib

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// Unzip a .zip archive to a specified directory
func (etlx *ETLX) Unzip(zipPath string, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	destRoot, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}
	destRoot = filepath.Clean(destRoot)
	destPrefix := destRoot + string(os.PathSeparator)

	for _, f := range r.File {
		outPath := filepath.Join(destRoot, f.Name)
		outPath = filepath.Clean(outPath)
		if outPath != destRoot && !strings.HasPrefix(outPath, destPrefix) {
			return fmt.Errorf("invalid zip entry path: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(outPath, os.ModePerm); err != nil {
				return err
			}
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return err
		}
		defer rc.Close()

		if err := os.MkdirAll(filepath.Dir(outPath), os.ModePerm); err != nil {
			return err
		}

		outFile, err := os.Create(outPath)
		if err != nil {
			return err
		}
		defer outFile.Close()

		if _, err = io.Copy(outFile, rc); err != nil {
			return err
		}
	}
	return nil
}

// Decompress a GZ file into the original file
func (etlx *ETLX) DecompressGZ(gzPath string, outputPath string) error {
	inFile, err := os.Open(gzPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	gzReader, err := gzip.NewReader(inFile)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	_, err = io.Copy(outFile, gzReader)
	return err
}
