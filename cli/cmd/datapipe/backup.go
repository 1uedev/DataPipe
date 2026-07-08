// backup.go implements OBS-150's CLI half: "datapipe backup export" and
// "datapipe backup restore" against a running control plane's REST API
// (GET/POST /backup, /backup/restore — docs/api/openapi.yaml). This is the
// CLI's first control-plane HTTP client; kept intentionally minimal (no
// retries, no session persistence) rather than growing a general-purpose
// API client package for a single pair of commands.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
)

func runBackup(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: datapipe backup export|restore")
	}
	switch args[0] {
	case "export":
		return runBackupExport(args[1:])
	case "restore":
		return runBackupRestore(args[1:])
	default:
		return fmt.Errorf("usage: datapipe backup export|restore")
	}
}

func runBackupExport(args []string) error {
	fs := flag.NewFlagSet("backup export", flag.ContinueOnError)
	url := fs.String("url", envOr("DATAPIPE_URL", "http://localhost:8080/api/v1"), "control plane base URL")
	username := fs.String("username", "", "System Admin username (or set DATAPIPE_TOKEN to skip login)")
	password := fs.String("password", "", "password for -username")
	out := fs.String("out", "datapipe-backup.json", "output file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	token, err := resolveToken(*url, *username, *password)
	if err != nil {
		return err
	}

	body, status, err := doRequest(http.MethodGet, *url+"/backup", token, nil)
	if err != nil {
		return fmt.Errorf("GET /backup: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("GET /backup: status %d: %s", status, body)
	}
	if err := os.WriteFile(*out, body, 0o600); err != nil {
		return fmt.Errorf("writing %s: %w", *out, err)
	}
	fmt.Printf("backup written to %s\n", *out)
	return nil
}

func runBackupRestore(args []string) error {
	fs := flag.NewFlagSet("backup restore", flag.ContinueOnError)
	url := fs.String("url", envOr("DATAPIPE_URL", "http://localhost:8080/api/v1"), "control plane base URL")
	username := fs.String("username", "", "System Admin username (or set DATAPIPE_TOKEN to skip login)")
	password := fs.String("password", "", "password for -username")
	in := fs.String("in", "datapipe-backup.json", "input backup file path")
	yes := fs.Bool("yes", false, "confirm this destructive, full-configuration-replacing restore")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if !*yes {
		return fmt.Errorf("restore replaces ALL current configuration on the target control plane; re-run with -yes to confirm")
	}

	token, err := resolveToken(*url, *username, *password)
	if err != nil {
		return err
	}

	bundleJSON, err := os.ReadFile(*in)
	if err != nil {
		return fmt.Errorf("reading %s: %w", *in, err)
	}
	var bundle json.RawMessage
	if err := json.Unmarshal(bundleJSON, &bundle); err != nil {
		return fmt.Errorf("parsing %s: %w", *in, err)
	}

	reqBody, err := json.Marshal(map[string]any{"confirm": true, "bundle": bundle})
	if err != nil {
		return fmt.Errorf("encoding restore request: %w", err)
	}
	body, status, err := doRequest(http.MethodPost, *url+"/backup/restore", token, reqBody)
	if err != nil {
		return fmt.Errorf("POST /backup/restore: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("POST /backup/restore: status %d: %s", status, body)
	}
	fmt.Println("restore complete — every prior session (including the one just used) is now invalid; log in again")
	return nil
}

// resolveToken returns DATAPIPE_TOKEN if set, otherwise logs in with
// username/password against baseURL + "/auth/login".
func resolveToken(baseURL, username, password string) (string, error) {
	if token := os.Getenv("DATAPIPE_TOKEN"); token != "" {
		return token, nil
	}
	if username == "" || password == "" {
		return "", fmt.Errorf("set DATAPIPE_TOKEN, or pass -username and -password")
	}
	reqBody, err := json.Marshal(map[string]string{"username": username, "password": password})
	if err != nil {
		return "", err
	}
	body, status, err := doRequest(http.MethodPost, baseURL+"/auth/login", "", reqBody)
	if err != nil {
		return "", fmt.Errorf("login: %w", err)
	}
	if status != http.StatusOK {
		return "", fmt.Errorf("login: status %d: %s", status, body)
	}
	var resp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("parsing login response: %w", err)
	}
	return resp.Token, nil
}

func doRequest(method, url, token string, body []byte) ([]byte, int, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, 0, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return respBody, resp.StatusCode, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
