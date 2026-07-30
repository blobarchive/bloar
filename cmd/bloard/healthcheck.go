package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const readinessHealthcheckTimeout = 3 * time.Second

// runReadinessHealthcheck asks the daemon's private readiness endpoint whether
// every configured serving gate is met. It is deliberately a one-shot client:
// Compose invokes this copy in the same container as the already-running
// daemon, and a non-200 response marks the container unready without killing
// the process that owns the store.
func runReadinessHealthcheck(listen string) error {
	target, err := readinessURL(listen)
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: readinessHealthcheckTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	req, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("building readiness request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("checking readiness: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readiness returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func readinessURL(listen string) (string, error) {
	if listen == "" {
		return "", fmt.Errorf("server.metrics_listen is empty")
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", fmt.Errorf("server.metrics_listen %q: %w", listen, err)
	}
	switch host {
	case "", "0.0.0.0":
		host = "127.0.0.1"
	case "::":
		host = "::1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/readyz", nil
}
