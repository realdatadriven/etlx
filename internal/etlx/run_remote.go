package etlxlib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Runner struct {
	client *ssh.Client
}

func NewSSH(host, user, keyFile string) (*Runner, error) {
	key, err := os.ReadFile(os.ExpandEnv(keyFile))
	if err != nil {
		key = []byte(keyFile)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}
	cfg := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	client, err := ssh.Dial("tcp", host, cfg)
	if err != nil {
		return nil, err
	}
	return &Runner{
		client: client,
	}, nil
}

func (r *Runner) Close() error {
	return r.client.Close()
}

func (r *Runner) Ping(ctx context.Context) error {
	session, err := r.client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	return nil
}

// upload file
func (r *Runner) Upload(ctx context.Context, localPath, remotePath string) error {
	client, err := sftp.NewClient(r.client)
	if err != nil {
		return fmt.Errorf("SFTP client creation failed: %w", err)
	}
	defer client.Close()
	srcFile, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("could not open source file: %w", err)
	}
	defer srcFile.Close()
	dstFile, err := client.Create(remotePath)
	if err != nil {
		return fmt.Errorf("could not create remote file: %w", err)
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("upload failed: %w", err)
	}
	return nil
}

func (r *Runner) Download(ctx context.Context, localPath, remotePath string) error {
	client, err := sftp.NewClient(r.client)
	if err != nil {
		return fmt.Errorf("SFTP client creation failed: %w", err)
	}
	defer client.Close()
	srcFile, err := client.Open(remotePath)
	if err != nil {
		return fmt.Errorf("could not open remote file: %w", err)
	}
	defer srcFile.Close()
	dstFile, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("could not create local file: %w", err)
	}
	defer dstFile.Close()
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	return nil
}

func (r *Runner) Run(ctx context.Context, cmd string) error {
	session, err := r.client.NewSession()
	if err != nil {
		return err
	}
	defer session.Close()
	stdout, err := session.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := session.StderrPipe()
	if err != nil {
		return err
	}
	if err := session.Start(cmd); err != nil {
		return err
	}
	go io.Copy(os.Stdout, stdout)
	go io.Copy(os.Stderr, stderr)
	done := make(chan error, 1)
	go func() {
		done <- session.Wait()
	}()
	select {
	case <-ctx.Done():
		_ = session.Signal(ssh.SIGTERM)
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (r *Runner) RunOutput(ctx context.Context, cmd string) (string, error) {
	session, err := r.client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()
	var out bytes.Buffer
	session.Stdout = &out
	session.Stderr = &out
	err = session.Run(cmd)
	return out.String(), err
}

func (r *Runner) systemctl(ctx context.Context, action, service string) error {
	return r.Run(ctx, fmt.Sprintf("systemctl --user %s %s", action, service))
}

func (r *Runner) Start(ctx context.Context, service string) error {
	return r.systemctl(ctx, "start", service)
}

func (r *Runner) Stop(ctx context.Context, service string) error {
	return r.systemctl(ctx, "stop", service)
}

func (r *Runner) Restart(ctx context.Context, service string) error {
	return r.systemctl(ctx, "restart", service)
}

func (r *Runner) Enable(ctx context.Context, service string) error {
	return r.systemctl(ctx, "enable", service)
}

func (r *Runner) Disable(ctx context.Context, service string) error {
	return r.systemctl(ctx, "disable", service)
}

func (r *Runner) Status(ctx context.Context, service string) (string, error) {
	return r.RunOutput(ctx, fmt.Sprintf("systemctl --user status %s --no-pager", service))
}

func (r *Runner) Logs(ctx context.Context, service string, lines int) (string, error) {
	return r.RunOutput(ctx, fmt.Sprintf("journalctl --user -u %s -n %d --no-pager", service, lines))
}

func (etlx *ETLX) RunREMOTE(dateRef []time.Time, conf map[string]any, extraConf map[string]any, keys ...string) ([]map[string]any, error) {
	key := "REMOTE"
	process := "REMOTE"
	if len(keys) > 0 && keys[0] != "" {
		key = keys[0]
	}
	//fmt.Println(key, dateRef)
	var processLogs []map[string]any
	start := time.Now().In(etlx.TimeZone)
	mem_alloc, mem_total_alloc, mem_sys, num_gc := etlx.RuntimeMemStats()
	processLogs = append(processLogs, map[string]any{
		"process": process,
		"name":    key,
		"key":     key, "start_at": start,
		"ref":                   nil,
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
	})
	// Check if the input conf is nil or empty
	if conf == nil {
		conf = etlx.Config
	}
	data, ok := conf[key].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing or invalid %s section", key)
	}
	// Extract metadata
	metadata, ok := data["metadata"].(map[string]any)
	if !ok {
		return nil, fmt.Errorf("missing metadata in %s section", key)
	}
	// ACTIVE
	if active, okActive := metadata["active"]; okActive {
		if !active.(bool) {
			log2 := map[string]any{
				"process":     process,
				"name":        fmt.Sprintf("KEY %s", key),
				"description": metadata["description"].(string),
				"key":         key,
				"start_at":    time.Now().In(etlx.TimeZone),
				"end_at":      time.Now().In(etlx.TimeZone),
				"success":     true,
				"msg":         "Deactivated",
			}
			processLogs = append(processLogs, log2)
			formatProcessLogEntry(log2)
			return nil, fmt.Errorf("%s deactivated", key)
		}
	}
	dtRef, okDtRef := metadata["date_ref"]
	if okDtRef && dtRef != "" {
		_dt, err := time.Parse("2006-01-02", dtRef.(string))
		if err == nil {
			dateRef = append([]time.Time{}, _dt)
		}
	} else {
		if len(dateRef) > 0 {
			dtRef = dateRef[0].Format("2006-01-02")
		}
	}
	if processLogs[0]["ref"] == nil {
		processLogs[0]["ref"] = dtRef
	}
	conn, okCon := metadata["connection"].(string)
	if !okCon {
		conn, okCon = metadata["conn"].(string)
		if !okCon {
			return nil, fmt.Errorf("%s err no connection defined", key)
		}
	}
	start3 := time.Now().In(etlx.TimeZone)
	mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
	_log2 := map[string]any{
		"process":               process,
		"name":                  key,
		"description":           metadata["description"].(string),
		"key":                   key,
		"start_at":              start3,
		"ref":                   dtRef,
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
	}
	dbConn, err := etlx.GetDB(conn)
	mem_alloc, mem_total_alloc, mem_sys, num_gc = etlx.RuntimeMemStats()
	_log2["mem_alloc_end"] = mem_alloc
	_log2["mem_total_alloc_end"] = mem_total_alloc
	_log2["mem_sys_end"] = mem_sys
	_log2["num_gc_end"] = num_gc
	if err != nil {
		_log2["success"] = false
		_log2["msg"] = fmt.Sprintf("%s ERR: connecting to %s in : %s", key, conn, err)
		_log2["end_at"] = time.Now().In(etlx.TimeZone)
		_log2["duration"] = time.Since(start3).Seconds()
		processLogs = append(processLogs, _log2)
		formatProcessLogEntry(_log2)
		return nil, fmt.Errorf("%s ERR: connecting to %s in : %s", key, conn, err)
	}
	defer dbConn.Close()
	// fmt.Println("CONN:", conn)
	order := []string{}
	__order, okOrder := data["__order"].([]any)
	if !okOrder {
		for key := range data {
			order = append(order, key)
		}
	} else {
		for _, itemKey := range __order {
			order = append(order, itemKey.(string))
		}
	}

	for _, itemKey := range order {
		if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
			continue
		}
		// fmt.Println("ITEM KEY:", itemKey)
		item := data[itemKey]
		if _, isMap := item.(map[string]any); !isMap {
			continue
		}
		itemMetadata, ok := item.(map[string]any)["metadata"]
		if !ok {
			continue
		}
		// ACTIVE
		if active, okActive := itemMetadata.(map[string]any)["active"]; okActive {
			if !active.(bool) {
				continue
			}
		}
		host, ok := itemMetadata.(map[string]any)["host"].(string)
		if !ok {
			continue
		}
		port := "22"
		if p, ok := itemMetadata.(map[string]any)["port"]; ok {
			port = fmt.Sprintf("%v", p)
		}
		user, _ := itemMetadata.(map[string]any)["user"].(string)
		keyFile, ok := itemMetadata.(map[string]any)["key"].(string) //.(map[string]any)
		if !ok {
			continue
		}
		desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
		if !okDesc {
			desc = fmt.Sprintf("%s->%s", key, itemKey)
		}
		sshIntance, err := NewSSH(fmt.Sprintf(`%s:%s`, host, port), user, keyFile)
		if err != nil {
			return nil, fmt.Errorf("SSH connection error in %s section", key)
		}
		fmt.Println(desc, sshIntance)
	}
	mem_alloc2, mem_total_alloc2, mem_sys2, num_gc2 := etlx.RuntimeMemStats()
	processLogs[0] = map[string]any{
		"process":               process,
		"name":                  key,
		"description":           metadata["description"].(string),
		"key":                   key,
		"start_at":              processLogs[0]["start_at"],
		"end_at":                time.Now().In(etlx.TimeZone),
		"duration":              time.Since(start).Seconds(),
		"mem_alloc_start":       mem_alloc,
		"mem_total_alloc_start": mem_total_alloc,
		"mem_sys_start":         mem_sys,
		"num_gc_start":          num_gc,
		"mem_alloc_end":         mem_alloc2,
		"mem_total_alloc_end":   mem_total_alloc2,
		"mem_sys_end":           mem_sys2,
		"num_gc_end":            num_gc2,
	}
	return processLogs, nil
}
