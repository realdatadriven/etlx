package etlxlib

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type Runner struct {
	client *ssh.Client
}

func NewSSH(host, user, keyFile, hostKey string) (*Runner, error) {
	key, err := os.ReadFile(os.ExpandEnv(keyFile))
	if err != nil {
		key = []byte(keyFile)
	}
	signer, err := ssh.ParsePrivateKey(key)
	if err != nil {
		return nil, err
	}

	hostKeyBytes, err := os.ReadFile(os.ExpandEnv(hostKey))
	if err != nil {
		hostKeyBytes = []byte(hostKey)
	}
	hostPublicKey, _, _, _, err := ssh.ParseAuthorizedKey(hostKeyBytes)
	if err != nil {
		return nil, err
	}

	cfg := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.PublicKeys(signer),
		},
		HostKeyCallback: ssh.FixedHostKey(hostPublicKey),
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

type remoteExecutionJob struct {
	name          string
	host          string
	port          string
	user          string
	keyFile       string
	workingDir    string
	commands      []any
	uploadFiles   []any
	downloadFiles []any
	run           []any
	description   string
	key           string
	item          map[string]any
	md            string
}

func runRemoteJobs(jobs []remoteExecutionJob, fn func(remoteExecutionJob) error) error {
	if len(jobs) == 0 {
		return nil
	}

	results := make(chan error, len(jobs))
	var wg sync.WaitGroup
	wg.Add(len(jobs))

	for _, job := range jobs {
		job := job
		go func() {
			defer wg.Done()
			results <- fn(job)
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for err := range results {
		if err != nil {
			return err
		}
	}
	return nil
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
	remote_executed := []string{}
	var jobs []remoteExecutionJob
	for _, itemKey := range order {
		if itemKey == "metadata" || itemKey == "__order" || itemKey == "order" {
			continue
		}
		item := data[itemKey]
		if _, isMap := item.(map[string]any); !isMap {
			continue
		}
		itemMetadata, ok := item.(map[string]any)["metadata"]
		if !ok {
			continue
		}
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
		keyFile, ok := itemMetadata.(map[string]any)["key"].(string)
		if !ok {
			continue
		}
		working_dir, ok := itemMetadata.(map[string]any)["working_dir"].(string)
		if !ok {
			return nil, fmt.Errorf("no working_dir %s section %s", key, itemKey)
		}
		run, ok := itemMetadata.(map[string]any)["run"].([]any)
		if !ok {
			return nil, fmt.Errorf("there was not specifc actions to run in %s section %s", key, itemKey)
		}
		if len(run) == 0 {
			return nil, fmt.Errorf("there was not specifc actions to run in %s section %s", key, itemKey)
		}
		for _, _run := range run {
			remote_executed = append(remote_executed, _run.(string))
		}
		commands, ok := itemMetadata.(map[string]any)["commands"].([]any)
		if !ok {
			return nil, fmt.Errorf("no commands %s section %s", key, itemKey)
		}
		upload_files, ok := itemMetadata.(map[string]any)["upload_files"].([]any)
		if !ok {
			upload_files = []any{}
		}
		_file, err := etlx.TempFIle("", etlx.MD, "pipeline.*.md")
		if err != nil {
			return nil, err
		}
		upload_files = append(upload_files, map[string]any{"source": _file, "dest": "pipeline.md"})
		download_files, _ := itemMetadata.(map[string]any)["download_files"].([]any)
		desc, okDesc := itemMetadata.(map[string]any)["description"].(string)
		if !okDesc {
			desc = fmt.Sprintf("%s->%s", key, itemKey)
		}
		jobs = append(jobs, remoteExecutionJob{
			name:          itemKey,
			host:          host,
			port:          port,
			user:          user,
			keyFile:       keyFile,
			workingDir:    working_dir,
			commands:      commands,
			uploadFiles:   upload_files,
			downloadFiles: download_files,
			description:   desc,
			key:           key,
			item:          item.(map[string]any),
			md:            etlx.MD,
			run:           run,
		})
	}
	err := runRemoteJobs(jobs, func(job remoteExecutionJob) error {
		sshInstance, err := NewSSH(fmt.Sprintf(`%s:%s`, job.host, job.port), job.user, job.keyFile)
		if err != nil {
			return fmt.Errorf("SSH connection error in %s section %s", key, job.name)
		}
		defer sshInstance.Close()
		if job.workingDir != "" {
			err := sshInstance.Run(context.Background(), fmt.Sprintf(`mkdir -p %s`, job.workingDir))
			if err != nil {
				return fmt.Errorf("SSH Err working dir error in %s section %s %s", key, job.name, err.Error())
			}
			err = sshInstance.Run(context.Background(), fmt.Sprintf(`cd %s`, job.workingDir))
			if err != nil {
				return fmt.Errorf("SSH Err cd to working dir error in %s section %s %s", key, job.name, err.Error())
			}
		}
		if len(job.uploadFiles) > 0 {
			for _, _file := range job.uploadFiles {
				localPath, ok := _file.(map[string]any)["source"].(string)
				if !ok {
					return fmt.Errorf("upload_files error %s section %s source file %s", key, job.name, localPath)
				}
				if content, ok := job.item[localPath].(string); ok && content != "" {
					_file, err := etlx.TempFIle("", content, fmt.Sprintf("%s.*.md", localPath))
					if err == nil {
						localPath = _file
					}
				}
				localPath = etlx.ReplaceQueryStringDate(localPath, dateRef)
				remoteFile, ok := _file.(map[string]any)["dest"].(string)
				if !ok {
					return fmt.Errorf("upload_files error %s section %s dest file %s", key, job.name, remoteFile)
				}
				remoteFile = etlx.ReplaceQueryStringDate(remoteFile, dateRef)
				err := sshInstance.Upload(context.Background(), localPath, fmt.Sprintf(`%s/%s`, job.workingDir, remoteFile))
				if err != nil {
					return fmt.Errorf("SSH Err upload file in %s section %s %s %s", key, job.name, err.Error(), remoteFile)
				}
			}
		}
		if len(job.commands) > 0 {
			/*for _, _run := range job.run {
				fmt.Sprintf(`etlx --config pipeline.md --only %s`, _run)
			}*/
			for _, _cmd := range job.commands {
				err := sshInstance.Run(context.Background(), etlx.ReplaceQueryStringDate(_cmd.(string), dateRef))
				if err != nil {
					return fmt.Errorf("SSH Err runnig command %s in %s section %s %s", _cmd, key, job.name, err.Error())
				}
			}
		}
		if len(job.downloadFiles) > 0 {
			for _, _file := range job.downloadFiles {
				localPath, ok := _file.(map[string]any)["source"].(string)
				if !ok {
					return fmt.Errorf("download_files error %s section %s source file", key, job.name)
				}
				localPath = etlx.ReplaceQueryStringDate(localPath, dateRef)
				remoteFile, ok := _file.(map[string]any)["dest"].(string)
				if !ok {
					return fmt.Errorf("download_files error %s section %s est file", key, job.name)
				}
				remoteFile = etlx.ReplaceQueryStringDate(remoteFile, dateRef)
				err := sshInstance.Download(context.Background(), localPath, fmt.Sprintf(`%s/%s`, job.workingDir, remoteFile))
				if err != nil {
					return fmt.Errorf("SSH Err download file in %s section %s %s %s", key, job.name, err.Error(), remoteFile)
				}
			}
		}
		//fmt.Println(job.description, sshInstance, job.workingDir, job.commands, job.uploadFiles, job.downloadFiles)
		return nil
	})
	if err != nil {
		return nil, err
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
	if _, ok := extraConf["skip"]; !ok {
		extraConf["skip"] = []string{}
	}
	for _, k := range remote_executed {
		extraConf["skip"] = append(extraConf["skip"].([]string), k)
	}
	delete(etlx.Config, key)
	logs, err := etlx.RunETLX(extraConf, dateRef)
	if err != nil {
		return nil, err
	}
	for _, l := range logs {
		processLogs = append(processLogs, l)
	}
	return processLogs, nil
}
