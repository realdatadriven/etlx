package etlxlib

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"html/template"
	"mime/multipart"
	"mime/quotedprintable"
	"net/smtp"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
)

// parseSlice converts an interface{} into a []string safely
func parseSlice(value any) []string {
	if value == nil {
		return nil
	}
	slice, ok := value.([]any)
	if !ok {
		return nil
	}
	var result []string
	for _, v := range slice {
		if str, ok := v.(string); ok {
			result = append(result, str)
		}
	}
	return result
}

// renderTemplate processes the HTML template with the provided data
func (etlx *ETLX) RenderTemplate(tmplStr string, data map[string]any) (string, error) {
	tmpl, err := template.New("email").Parse(tmplStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %v", err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute template: %v", err)
	}
	return buf.String(), nil
}

// sendEmail sends an email with dynamic HTML content, optional CC, BCC, and attachments
func (etlx *ETLX) SendEmail(data map[string]any) error {
	// Load SMTP configuration from environment variables
	smtpHost := os.Getenv("SMTP_HOST")
	smtpPort := os.Getenv("SMTP_PORT")
	smtpUsername := os.Getenv("SMTP_USERNAME")
	smtpPassword := os.Getenv("SMTP_PASSWORD")
	smtpFrom := os.Getenv("SMTP_FROM")

	// Extract fields from data
	to := parseSlice(data["to"])
	cc := parseSlice(data["cc"])
	bcc := parseSlice(data["bcc"])
	subject, _ := data["subject"].(string)
	bodyTemplate, _ := data["body"].(string)
	templateData, _ := data["data"].(map[string]any)
	attachments := parseSlice(data["attachments"])

	if len(to) == 0 {
		return fmt.Errorf("recipient 'to' field is required")
	}

	// Render the HTML template with data
	body, err := etlx.RenderTemplate(bodyTemplate, templateData)
	if err != nil {
		return err
	}

	// SMTP authentication
	auth := smtp.PlainAuth("", smtpUsername, smtpPassword, smtpHost)

	// Create email buffer
	var email bytes.Buffer
	writer := multipart.NewWriter(&email)
	boundary := writer.Boundary()

	// Headers
	headers := map[string]string{
		"From":         smtpFrom,
		"To":           strings.Join(to, ", "),
		"Subject":      subject,
		"MIME-Version": "1.0",
		"Content-Type": fmt.Sprintf("multipart/mixed; boundary=%s", boundary),
	}
	if len(cc) > 0 {
		headers["Cc"] = strings.Join(cc, ", ")
	}

	// Write headers
	for key, val := range headers {
		email.WriteString(fmt.Sprintf("%s: %s\r\n", key, val))
	}
	email.WriteString("\r\n")
	// Add HTML body
	htmlPart, _ := writer.CreatePart(textproto.MIMEHeader{
		"Content-Type":              {"text/html; charset=UTF-8"},
		"Content-Transfer-Encoding": {"quoted-printable"},
	})
	qpWriter := quotedprintable.NewWriter(htmlPart)
	qpWriter.Write([]byte(body))
	qpWriter.Close()
	// Attach files
	if len(attachments) > 0 {
		for _, attachmentPath := range attachments {
			file, err := os.Open(attachmentPath)
			if err != nil {
				return fmt.Errorf("failed to open attachment %s: %v", attachmentPath, err)
			}
			defer file.Close()

			// Read file content
			fileContent, err := os.ReadFile(attachmentPath)
			if err != nil {
				return fmt.Errorf("failed to read attachment %s: %v", attachmentPath, err)
			}

			// Create attachment part
			fileName := filepath.Base(attachmentPath)
			attachmentHeader := textproto.MIMEHeader{
				"Content-Type":              {"application/octet-stream"},
				"Content-Disposition":       {fmt.Sprintf("attachment; filename=\"%s\"", fileName)},
				"Content-Transfer-Encoding": {"base64"},
			}
			attachmentPart, _ := writer.CreatePart(attachmentHeader)

			// Encode file content as base64
			encoded := base64.StdEncoding.EncodeToString(fileContent)
			attachmentPart.Write([]byte(encoded))
		}
	}
	// Close writer
	writer.Close()
	// Merge recipients
	recipients := append(to, append(cc, bcc...)...)
	// Send email
	serverAddr := smtpHost + ":" + smtpPort
	err = smtp.SendMail(serverAddr, auth, smtpUsername, recipients, email.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send email: %v", err)
	}
	return nil
}
