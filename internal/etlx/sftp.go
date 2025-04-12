package etlxlib

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// getHostKey loads and parses the host public key
func getHostKey(path string) (ssh.PublicKey, error) {
	hostKeyBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read host key file: %w", err)
	}
	hostKey, _, _, _, err := ssh.ParseAuthorizedKey(hostKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse host key: %w", err)
	}
	return hostKey, nil
}

// runSFTPActionWithFixedHostKey uploads or downloads files via SFTP with host key validation
func (etlx *ETLX) SFTPActionWithFixedHostKey(mode string, params map[string]any) error {
	// Extract and validate required params
	host, _ := params["host"].(string)
	user, _ := params["user"].(string)
	password, _ := params["password"].(string)
	source, _ := params["source"].(string)
	target, _ := params["target"].(string)
	hostKeyPath, _ := params["host_key"].(string)
	port := 22
	if p, ok := params["port"].(int); ok {
		port = p
	}
	if host == "" || user == "" || password == "" || source == "" || target == "" || hostKeyPath == "" {
		return fmt.Errorf("missing required SFTP parameters: host, user, password, source, target, host_key")
	}
	host = etlx.ReplaceEnvVariable(host)
	user = etlx.ReplaceEnvVariable(user)
	password = etlx.ReplaceEnvVariable(password)
	// Get host key for validation
	hostKey, err := getHostKey(hostKeyPath)
	if err != nil {
		return fmt.Errorf("could not load host key: %w", err)
	}

	// Create SSH config
	config := &ssh.ClientConfig{
		User:            user,
		Auth:            []ssh.AuthMethod{ssh.Password(password)},
		HostKeyCallback: ssh.FixedHostKey(hostKey),
		Timeout:         5 * time.Second,
	}

	// Connect
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return fmt.Errorf("SSH dial failed: %w", err)
	}
	defer conn.Close()

	// Create SFTP client
	client, err := sftp.NewClient(conn)
	if err != nil {
		return fmt.Errorf("SFTP client creation failed: %w", err)
	}
	defer client.Close()

	switch mode {
	case "upload":
		srcFile, err := os.Open(source)
		if err != nil {
			return fmt.Errorf("could not open source file: %w", err)
		}
		defer srcFile.Close()

		dstFile, err := client.Create(target)
		if err != nil {
			return fmt.Errorf("could not create remote file: %w", err)
		}
		defer dstFile.Close()

		_, err = io.Copy(dstFile, srcFile)
		if err != nil {
			return fmt.Errorf("upload failed: %w", err)
		}
	case "download":
		srcFile, err := client.Open(source)
		if err != nil {
			return fmt.Errorf("could not open remote file: %w", err)
		}
		defer srcFile.Close()

		dstFile, err := os.Create(target)
		if err != nil {
			return fmt.Errorf("could not create local file: %w", err)
		}
		defer dstFile.Close()

		_, err = io.Copy(dstFile, srcFile)
		if err != nil {
			return fmt.Errorf("download failed: %w", err)
		}
	default:
		return fmt.Errorf("unsupported SFTP action: %s", mode)
	}

	return nil
}
