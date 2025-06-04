package etlxlib

import (
	"fmt"
	"io"
	"os"
	"time"
	"path"
	"path/filepath"

	"github.com/jlaffaye/ftp"
)

func (etlx *ETLX) FTPUpload(host string, port string, user, pass, localPath, remotePath string) error {
	if port == "" {
		port = "21"
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
		port = "21"
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

func globToRegex(glob string) string {
	var sb strings.Builder
	sb.WriteString("^")
	for i := 0; i < len(glob); i++ {
		switch glob[i] {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		case '.', '(', ')', '+', '|', '^', '$', '[', ']', '{', '}', '\\':
			sb.WriteString(`\`)
			sb.WriteByte(glob[i])
		default:
			sb.WriteByte(glob[i])
		}
	}
	sb.WriteString("$")
	return sb.String()
}

func (etlx *ETLX) FTPDownloadBatch(host, port, user, pass, remoteDir, pattern, localDir string) error {
	if port == "" {
		port = "21"
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

	// List all files in the remote directory
	entries, err := conn.List(remoteDir)
	if err != nil {
		return fmt.Errorf("failed to list remote directory: %w", err)
	}

	// Ensure local directory exists
	if err := os.MkdirAll(localDir, 0755); err != nil {
		return fmt.Errorf("failed to create local directory: %w", err)
	}

	// Loop over files and download matching ones
	for _, entry := range entries {
		if entry.Type != ftp.EntryTypeFile {
			continue // skip non-files
		}

		matched, err := filepath.Match(globToRegex(pattern), entry.Name)
		if err != nil {
			return fmt.Errorf("invalid pattern: %w", err)
		}
		if matched {
			remotePath := path.Join(remoteDir, entry.Name)
			localPath := filepath.Join(localDir, entry.Name)

			fmt.Printf("Downloading: %s → %s\n", remotePath, localPath)

			// Download each matching file using the single-file method
			err := etlx.FTPDownload(host, port, user, pass, remotePath, localPath)
			if err != nil {
				return fmt.Errorf("failed to download %s: %w", remotePath, err)
			}
		}
	}

	return nil
}
