package etlxlib

import (
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

func (etlx *ETLX) ReadEmails(cfg map[string]any) ([]map[string]any, error) {
	host := cfg["host"].(string)
	port := cfg["port"].(int)
	username := cfg["username"].(string)
	password := cfg["password"].(string)
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
	c, err := client.DialTLS(fmt.Sprintf("%s:%d", host, port), nil)
	if err != nil {
		return nil, err
	}
	defer c.Logout()
	// Login
	err = c.Login(username, password)
	if err != nil {
		return nil, err
	}
	// Select mailbox
	_, err = c.Select(folder, false)
	if err != nil {
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
				criteria.Header.Add("Subject", fmt.Sprint(value))
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
		return nil, err
	}
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
						email["attachments"] = files
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

/*func main(){
	cfg := map[string]any{
		"protocol": "IMAP",
		"host": "imap.gmail.com",
		"port": 993,
		"username": "user@gmail.com",
		"password": "secret",
		"folder":
			"INBOX",


		"download_att":
			true,


		"attachment_path":
			"./downloads",



		"search":
			map[string]any{

				"from":
					"supplier@example.com",


				"subject":
					"Invoice",


				"since":
					"24h",
			},
	}



	emails, err :=
		ReadEmails(cfg)


	if err != nil {
		panic(err)
	}



	for _, email :=
		range emails {

		fmt.Printf(
			"%+v\n\n",
			email,
		)

	}

}*/
