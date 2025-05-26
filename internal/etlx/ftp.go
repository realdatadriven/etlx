package etlxlib

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jlaffaye/ftp"
)

func (etlx *ETLX) FTPUpload(host string, port string, user, pass, localPath, remotePath string) error {
	if port == "" {
		port = "25"
	}
	address := host + ":" + port
	conn, err := ftp.Dial(address, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer conn.Quit()
	if user != "" && pass != "" {
		if err := conn.Login(user, pass); err != nil {
			return fmt.Errorf("failed to login: %w", err)
		}
	}
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file: %w", err)
	}
	defer file.Close()
	if err := conn.Stor(remotePath, file); err != nil {
		return fmt.Errorf("failed to upload: %w", err)
	}
	return nil
}

func (etlx *ETLX) FTPDownload(host string, port string, user, pass, remotePath, localPath string) error {
	if port == "" {
		port = "25"
	}
	address := host + ":" + port
	conn, err := ftp.Dial(address, ftp.DialWithTimeout(5*time.Second))
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}
	defer conn.Quit()
	if user != "" && pass != "" {
		if err := conn.Login(user, pass); err != nil {
			return fmt.Errorf("failed to login: %w", err)
		}
	}
	response, err := conn.Retr(remotePath)
	if err != nil {
		return fmt.Errorf("failed to download file: %w", err)
	}
	defer response.Close()
	outFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer outFile.Close()
	_, err = io.Copy(outFile, response)
	if err != nil {
		return fmt.Errorf("failed to save file: %w", err)
	}
	return nil
}
