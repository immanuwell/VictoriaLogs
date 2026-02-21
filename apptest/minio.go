package apptest

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	MinioRootUser     = "minioadmin"
	MinioRootPassword = "minioadmin"
	MinioBucket       = "vltest"
)

type Minio struct {
	process  *os.Process
	endpoint string
}

func TryStartMinio(t *testing.T, instance string) (*Minio, bool) {
	t.Helper()

	binary := "../../bin/minio"
	if _, err := os.Stat(binary); err != nil {
		return nil, false
	}

	dataPath := filepath.Join(t.Name(), instance)
	bucketPath := filepath.Join(dataPath, MinioBucket)
	if err := os.MkdirAll(bucketPath, 0o755); err != nil {
		t.Fatalf("cannot create MinIO bucket directory %q: %v", bucketPath, err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cannot find free port for MinIO: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	cmd := exec.Command(binary, "server", dataPath, "--address", addr)
	cmd.Env = append(os.Environ(),
		"MINIO_ROOT_USER="+MinioRootUser,
		"MINIO_ROOT_PASSWORD="+MinioRootPassword,
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("cannot start MinIO from %q: %v", binary, err)
	}

	healthURL := "http://" + addr + "/minio/health/live"
	httpClient := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(healthURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	m := &Minio{
		process:  cmd.Process,
		endpoint: "http://" + addr,
	}
	t.Cleanup(m.Stop)
	return m, true
}

func (m *Minio) Stop() {
	m.process.Signal(os.Interrupt) //nolint:errcheck
	m.process.Wait()               //nolint:errcheck
}

func (m *Minio) Endpoint() string {
	return m.endpoint
}

func (m *Minio) OffloadDestination() string {
	return fmt.Sprintf("s3://%s", MinioBucket)
}

func (m *Minio) OffloadFlags() []string {
	return []string{
		fmt.Sprintf("-offload.destination=%s", m.OffloadDestination()),
		fmt.Sprintf("-offload.s3.endpoint=%s", m.Endpoint()),
		"-offload.s3.forcePathStyle=true",
		"-offloadPeriod=24h",
	}
}
