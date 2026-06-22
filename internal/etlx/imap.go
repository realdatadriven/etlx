package etlxlib

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-message/mail"
)

func returnAdresses(imapAdress []*imap.Address) string {
	adress := ""
	for i, adr := range imapAdress {
		glue := ""
		if i > 0 {
			glue = ";"
		}
		adress += glue + adr.Address()
	}
	return adress
}

func (etlx *ETLX) ReadEmails(cfg map[string]any, item map[string]any, dateRef []time.Time) ([]map[string]any, error) {
	host := etlx.ReplaceEnvVariable(cfg["host"].(string))
	port := etlx.ReplaceEnvVariable(cfg["port"].(string))
	username := etlx.ReplaceEnvVariable(cfg["username"].(string))
	password := etlx.ReplaceEnvVariable(cfg["password"].(string))
	folder := "INBOX"
	if v, ok := cfg["folder"].(string); ok {
		folder = v
	}
	downloadAtt := false
	if v, ok := cfg["download_att"].(bool); ok {
		downloadAtt = v
	}
	attachmentPath := "./attachments"
	if v, ok := cfg["attachment_path"].(string); ok {
		attachmentPath = v
	}
	if downloadAtt {
		err := os.MkdirAll(attachmentPath, 0755)
		if err != nil {
			return nil, err
		}
	}
	// Connect IMAP
	c, err := client.DialTLS(fmt.Sprintf("%s:%s", host, port), nil)
	if err != nil {
		fmt.Println("client.DialTLS Err:", err)
		return nil, err
	}
	defer c.Logout()
	// Login
	err = c.Login(username, password)
	if err != nil {
		fmt.Println("c.Login Err:", err)
		return nil, err
	}
	// Select mailbox
	_, err = c.Select(folder, false)
	if err != nil {
		fmt.Println("c.Select(folder, false) Err:", err)
		return nil, err
	}
	// Build search
	criteria := imap.NewSearchCriteria()
	if search, ok := cfg["search"].(map[string]any); ok {
		for key, value := range search {
			switch strings.ToLower(key) {
			case "from":
				criteria.Header.Add("From", fmt.Sprint(value))
			case "subject":
				subj := etlx.SetQueryPlaceholders(fmt.Sprint(value), "", "", dateRef)
				criteria.Header.Add("Subject", subj)
			case "since":
				d, err := time.ParseDuration(fmt.Sprint(value))
				if err == nil {
					criteria.Since = time.Now().Add(-d)
				}
			case "before":
				d, err := time.ParseDuration(fmt.Sprint(value))
				if err == nil {
					criteria.Before = time.Now().Add(-d)
				}
			}
		}
	}
	ids, err := c.Search(criteria)
	if err != nil {
		fmt.Println("c.Search(criteria) Err:", err)
		return nil, err
	}
	// fmt.Println("Fine Till Search Criteria OK")
	results := []map[string]any{}
	for _, id := range ids {
		seq := new(imap.SeqSet)
		seq.AddNum(id)
		messages := make(chan *imap.Message, 1)
		go func() {
			c.Fetch(seq, []imap.FetchItem{imap.FetchEnvelope, imap.FetchRFC822}, messages)
		}()
		for msg := range messages {
			email := map[string]any{
				"id":          id,
				"subject":     msg.Envelope.Subject,
				"from":        msg.Envelope.From[0].Address(),
				"to":          returnAdresses(msg.Envelope.To),
				"cc":          returnAdresses(msg.Envelope.Cc),
				"bcc":         returnAdresses(msg.Envelope.Bcc),
				"date":        msg.Envelope.Date,
				"body":        "",
				"attachments": []string{},
			}
			if downloadAtt {
				body := msg.GetBody(&imap.BodySectionName{})
				if body != nil {
					text, files, err := parseEmail(body, attachmentPath)
					if err == nil {
						email["body"] = text
						if len(files) > 0 {
							filesJSON, _ := json.Marshal(files)
							email["attachments"] = string(filesJSON)
						} else {
							email["attachments"] = nil
						}
					}
				}
			}
			results = append(results, email)
		}
	}
	return results, nil
}

func parseEmail(r io.Reader, dir string) (string, []string, error) {
	mr, err := mail.CreateReader(r)
	if err != nil {
		return "", nil, err
	}
	body := ""
	files := []string{}
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return body, files, err
		}
		switch h := p.Header.(type) {
		case *mail.InlineHeader:
			data, _ := io.ReadAll(p.Body)
			body = string(data)
		case *mail.AttachmentHeader:
			filename, err := h.Filename()
			if err != nil {
				continue
			}
			path := filepath.Join(dir, filename)
			f, err := os.Create(path)
			if err != nil {
				continue
			}
			io.Copy(f, p.Body)
			f.Close()
			files = append(files, path)
		}
	}
	return body, files, nil
}
