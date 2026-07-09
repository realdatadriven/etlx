package etlxlib

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/chromedp/cdproto/page"
	"github.com/chromedp/chromedp"
)

// USING CHROMIUN
// print a specific pdf page.
func (etlx *ETLX) GenPDFFromHTML(html, output_path string) error {
	ctx, cancel := chromedp.NewContext(context.Background())
	defer cancel()
	base := filepath.Base(output_path)
	ext := filepath.Ext(base)
	base_no_ext := strings.Replace(base, ext, "", 1)
	temptex, err := os.CreateTemp("", fmt.Sprintf("%s-*.html", base_no_ext))
	if err != nil {
		return err
	}
	defer os.Remove(temptex.Name())
	defer temptex.Close()
	_, err = temptex.WriteString(html)
	if err != nil {
		return err
	}
	temptex.Close()
	var pdf []byte
	fmt.Println(html)
	err = chromedp.Run(ctx,
		chromedp.Navigate(fmt.Sprintf("file://%s", temptex.Name())),
		chromedp.ActionFunc(func(ctx context.Context) error {
			var err error
			pdf, _, err = page.PrintToPDF().
				WithPrintBackground(true).
				WithLandscape(false).
				WithMarginLeft(0.4).
				WithMarginTop(0.4).
				WithMarginRight(0.4).
				WithMarginBottom(0.4).
				Do(ctx)
			return err
		}),
	)
	err = os.WriteFile(output_path, pdf, 0644)
	if err != nil {
		return err
	}
	return nil
}

// sudo apt install texlive-latex-base texlive-latex-extra
func (etlx *ETLX) GenPDFFromLatex(latex, output_path string) error {
	dir := filepath.Dir(output_path)
	base := filepath.Base(output_path)
	ext := filepath.Ext(base)
	base_no_ext := strings.Replace(base, ext, "", 1)
	temptex, err := os.CreateTemp("", fmt.Sprintf("%s-*.tex", base_no_ext))
	if err != nil {
		return err
	}
	defer os.Remove(temptex.Name())
	defer temptex.Close()
	_, err = temptex.WriteString(latex)
	if err != nil {
		return err
	}
	temptex.Close()
	jobname := fmt.Sprintf("-jobname=%s", base_no_ext)
	output_directory := fmt.Sprintf("-output-directory=%s", dir)
	cmd := exec.Command("pdflatex", jobname, output_directory, temptex.Name())
	//err = cmd.Run()
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println("GenPDFFromLatex Err:", string(output), err)
		return fmt.Errorf("%s: %s", err.Error(), string(output))
	}
	return nil
}
