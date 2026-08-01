package kubo_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/kubo"
)

// compatibleCommandTree is a deliberately independent fixture for the narrow
// Kubo 0.42 RPC surface the client consumes. Keeping this in the external test
// package makes capability-list changes an explicit compatibility decision
// instead of letting tests mirror the implementation automatically.
var compatibleCommandTree = []string{
	"ipfs block get",
	"ipfs block put",
	"ipfs block put --allow-big-block",
	"ipfs block put --cid-codec",
	"ipfs block put --mhlen",
	"ipfs block put --mhtype",
	"ipfs block put --pin",
	"ipfs block rm",
	"ipfs block rm --force",
	"ipfs block rm --quiet",
	"ipfs block stat",
	"ipfs config",
	"ipfs config --expand-auto",
	"ipfs id",
	"ipfs key gen",
	"ipfs key gen --ipns-base",
	"ipfs key gen --type",
	"ipfs key ls",
	"ipfs key ls -l",
	"ipfs key ls --ipns-base",
	"ipfs name publish",
	"ipfs name publish --allow-delegated",
	"ipfs name publish --allow-offline",
	"ipfs name publish --ipns-base",
	"ipfs name publish --key",
	"ipfs name publish --lifetime",
	"ipfs name publish --quieter / ipfs name publish -Q",
	"ipfs name publish --resolve",
	"ipfs name publish --sequence",
	"ipfs name publish --ttl",
	"ipfs name publish --v1compat",
	"ipfs name resolve",
	"ipfs name resolve --dht-record-count",
	"ipfs name resolve --dht-timeout",
	"ipfs name resolve --nocache",
	"ipfs name resolve --recursive",
	"ipfs name resolve --stream",
	"ipfs pin add",
	"ipfs pin add --fast-provide-dag",
	"ipfs pin add --fast-provide-root",
	"ipfs pin add --fast-provide-wait",
	"ipfs pin add --name / ipfs pin add -n",
	"ipfs pin add --progress",
	"ipfs pin add --recursive",
	"ipfs pin ls",
	"ipfs pin ls --name / ipfs pin ls -n",
	"ipfs pin ls --names",
	"ipfs pin ls --quiet",
	"ipfs pin ls --stream",
	"ipfs pin ls --type",
	"ipfs pin rm",
	"ipfs pin rm --recursive",
	"ipfs pin update",
	"ipfs pin update --fast-provide-dag",
	"ipfs pin update --fast-provide-root",
	"ipfs pin update --fast-provide-wait",
	"ipfs pin update --unpin",
	"ipfs provide once",
	"ipfs provide once --recursive",
	"ipfs refs local",
	"ipfs repo gc",
	"ipfs repo gc --quiet",
	"ipfs repo gc --silent",
	"ipfs repo gc --stream-errors",
	"ipfs repo stat",
	"ipfs repo stat --human / ipfs repo stat -H",
	"ipfs repo stat --size-only",
	"ipfs repo verify",
	"ipfs repo verify --drop",
	"ipfs repo verify --heal",
	"ipfs repo verify --heal-timeout",
	"ipfs routing get",
	"ipfs swarm connect",
	"ipfs swarm peers",
	"ipfs swarm peers --direction",
	"ipfs swarm peers --identify",
	"ipfs swarm peers --latency",
	"ipfs swarm peers --streams",
	"ipfs swarm peers --verbose",
	"ipfs version",
}

var compatibleReplicaCommandTree = []string{
	"ipfs block get",
	"ipfs block put",
	"ipfs block put --allow-big-block",
	"ipfs block put --cid-codec",
	"ipfs block put --mhlen",
	"ipfs block put --mhtype",
	"ipfs block put --pin",
	"ipfs config",
	"ipfs config --expand-auto",
	"ipfs id",
	"ipfs pin add",
	"ipfs pin add --fast-provide-dag",
	"ipfs pin add --fast-provide-root",
	"ipfs pin add --fast-provide-wait",
	"ipfs pin add --name / ipfs pin add -n",
	"ipfs pin add --progress",
	"ipfs pin add --recursive",
	"ipfs pin ls",
	"ipfs pin ls --name / ipfs pin ls -n",
	"ipfs pin ls --names",
	"ipfs pin ls --quiet",
	"ipfs pin ls --stream",
	"ipfs pin ls --type",
	"ipfs pin rm",
	"ipfs pin rm --recursive",
	"ipfs pin update",
	"ipfs pin update --fast-provide-dag",
	"ipfs pin update --fast-provide-root",
	"ipfs pin update --fast-provide-wait",
	"ipfs pin update --unpin",
	"ipfs provide once",
	"ipfs provide once --recursive",
	"ipfs routing get",
	"ipfs swarm connect",
	"ipfs version",
}

func writeCompatibleDaemon(t *testing.T, w http.ResponseWriter, r *http.Request, version string) {
	t.Helper()
	if r.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", r.Method)
	}
	switch r.URL.Path {
	case "/api/v0/version":
		writeJSON(t, w, map[string]string{"Version": version})
	case "/api/v0/commands":
		if got := r.URL.Query().Get("encoding"); got != "text" {
			t.Errorf("commands encoding = %q, want text", got)
		}
		if got := r.URL.Query().Get("flags"); got != "true" {
			t.Errorf("commands flags = %q, want true", got)
		}
		w.Header().Set("Content-Type", "text/plain")
		if _, err := w.Write([]byte(strings.Join(compatibleCommandTree, "\n") + "\n")); err != nil {
			t.Errorf("write commands: %v", err)
		}
	default:
		http.NotFound(w, r)
	}
}

func compatibilityServer(t *testing.T, commands []byte) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/version":
			writeJSON(t, w, map[string]string{"Version": "0.42.0"})
		case "/api/v0/commands":
			w.Header().Set("Content-Type", "text/plain")
			if _, err := w.Write(commands); err != nil {
				t.Errorf("write commands: %v", err)
			}
		default:
			http.NotFound(w, r)
		}
	}))
}

func TestExplicitUnauthenticatedLoopbackMode(t *testing.T) {
	testServer := func(t *testing.T, tls bool) {
		t.Helper()
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if values, present := r.Header["Authorization"]; present || len(values) != 0 {
				t.Errorf("Authorization unexpectedly present: %q", values)
			}
			writeCompatibleDaemon(t, w, r, "0.42.0")
		})
		var server *httptest.Server
		if tls {
			server = httptest.NewTLSServer(handler)
		} else {
			server = httptest.NewServer(handler)
		}
		defer server.Close()
		cfg := kubo.Config{BaseURL: server.URL, AllowUnauthenticated: true}
		if tls {
			cfg.HTTPClient = server.Client()
		}
		client, err := kubo.New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, err := client.CheckCompatibility(t.Context()); err != nil {
			t.Fatalf("CheckCompatibility: %v", err)
		}
	}
	t.Run("HTTP", func(t *testing.T) { testServer(t, false) })
	t.Run("HTTPS", func(t *testing.T) { testServer(t, true) })

	credential := t.TempDir() + "/token"
	if err := os.WriteFile(credential, []byte(testToken), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	for name, cfg := range map[string]kubo.Config{
		"remote HTTPS": {
			BaseURL: "https://example.test", AllowUnauthenticated: true,
		},
		"remote HTTP despite override": {
			BaseURL: "http://example.test", AllowUnauthenticated: true, AllowInsecureHTTP: true,
		},
		"inline bearer": {
			BaseURL: "http://127.0.0.1:5001", AllowUnauthenticated: true, BearerToken: testToken,
		},
		"file bearer": {
			BaseURL: "http://127.0.0.1:5001", AllowUnauthenticated: true, BearerTokenFile: credential,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := kubo.New(cfg); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
}

func TestKuboCompatibilityGate(t *testing.T) {
	accepted := []string{
		"0.42.0",
		"v0.42.19",
		"0.42.3+vendor.1",
		"v0.42.7+linux-amd64.20260722",
	}
	for _, version := range accepted {
		t.Run("accept "+version, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				writeCompatibleDaemon(t, w, r, version)
			}))
			defer server.Close()
			info, err := newClient(t, server.URL, nil).CheckCompatibility(t.Context())
			if err != nil {
				t.Fatalf("CheckCompatibility: %v", err)
			}
			if info.Version != version {
				t.Fatalf("Version = %q, want %q", info.Version, version)
			}
		})
	}

	rejected := []string{
		"0.41.9",
		"0.43.0",
		"0.42",
		"0.42.0-rc.1",
		"0.42.0-dev",
		"V0.42.0",
		"0.42.00",
		"0.42.0+",
		"0.42.0+vendor..one",
		" 0.42.0",
		"0.42.0 ",
	}
	for _, version := range rejected {
		t.Run("reject "+version, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, map[string]string{"Version": version})
			}))
			defer server.Close()
			client := newClient(t, server.URL, nil)
			if info, err := client.Version(t.Context()); err != nil || info.Version != version {
				t.Fatalf("raw Version = %+v, %v", info, err)
			}
			_, err := client.CheckCompatibility(t.Context())
			var compatibility *kubo.CompatibilityError
			if !errors.As(err, &compatibility) || compatibility.Supported != kubo.SupportedKuboLine {
				t.Fatalf("error = %T %v, want CompatibilityError", err, err)
			}
		})
	}
}

func TestReplicaCompatibilityUsesLeastPrivilegeCapabilityProfile(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v0/version":
			writeJSON(t, w, map[string]string{"Version": "0.42.0"})
		case "/api/v0/commands":
			w.Header().Set("Content-Type", "text/plain")
			_, _ = w.Write([]byte(strings.Join(compatibleReplicaCommandTree, "\n") + "\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	if _, err := client.CheckReplicaCompatibility(t.Context()); err != nil {
		t.Fatalf("CheckReplicaCompatibility: %v", err)
	}
	if _, err := client.CheckCompatibility(t.Context()); err == nil {
		t.Fatal("broad CheckCompatibility accepted the replica-only command tree")
	} else {
		var capability *kubo.CapabilityError
		if !errors.As(err, &capability) || capability.Missing != "ipfs block rm" {
			t.Fatalf("broad error = %T %v, want missing block/rm", err, err)
		}
	}
}

func TestReplicaRuntimeCompatibilityExcludesConfigRPC(t *testing.T) {
	commands := make([]string, 0, len(compatibleReplicaCommandTree)-2)
	for _, command := range compatibleReplicaCommandTree {
		if command == "ipfs config" || command == "ipfs config --expand-auto" {
			continue
		}
		commands = append(commands, command)
	}
	server := compatibilityServer(t, []byte(strings.Join(commands, "\n")+"\n"))
	defer server.Close()
	client := newClient(t, server.URL, nil)
	if _, err := client.CheckReplicaRuntimeCompatibility(t.Context()); err != nil {
		t.Fatalf("runtime-only compatibility rejected a config-free command tree: %v", err)
	}
	if _, err := client.CheckReplicaCompatibility(t.Context()); err == nil {
		t.Fatal("provider-policy compatibility accepted a command tree without config")
	} else {
		var capability *kubo.CapabilityError
		if !errors.As(err, &capability) || capability.Missing != "ipfs config" {
			t.Fatalf("error = %T %v, want missing config", err, err)
		}
	}
}

func TestReplicaCompatibilityRejectsMissingReplicaCapability(t *testing.T) {
	for _, missing := range []string{"ipfs swarm connect", "ipfs version"} {
		t.Run(missing, func(t *testing.T) {
			commands := make([]string, 0, len(compatibleReplicaCommandTree)-1)
			for _, command := range compatibleReplicaCommandTree {
				if command != missing {
					commands = append(commands, command)
				}
			}
			server := compatibilityServer(t, []byte(strings.Join(commands, "\n")+"\n"))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).CheckReplicaCompatibility(t.Context())
			var capability *kubo.CapabilityError
			if !errors.As(err, &capability) || capability.Missing != missing {
				t.Fatalf("error = %T %v, want missing %q CapabilityError", err, err, missing)
			}
		})
	}
}

func TestCompatibilityRejectsMissingCapability(t *testing.T) {
	commands := []byte(strings.Join(compatibleCommandTree[:len(compatibleCommandTree)-1], "\n") + "\n")
	server := compatibilityServer(t, commands)
	defer server.Close()

	info, err := newClient(t, server.URL, nil).CheckCompatibility(t.Context())
	if info.Version != "0.42.0" {
		t.Fatalf("Version = %q, want 0.42.0", info.Version)
	}
	var capability *kubo.CapabilityError
	if !errors.As(err, &capability) || capability.Missing != "ipfs version" {
		t.Fatalf("error = %T %v, want missing-version CapabilityError", err, err)
	}
}

func TestCompatibilityRejectsMalformedCommandTree(t *testing.T) {
	valid := strings.Join(compatibleCommandTree, "\n") + "\n"
	tests := map[string][]byte{
		"duplicate":              []byte(valid + compatibleCommandTree[0] + "\n"),
		"surrounding whitespace": []byte(valid + " ipfs dag stat\n"),
		"invalid command case":   []byte(valid + "IPFS dag stat\n"),
		"invalid UTF-8":          append([]byte(valid), 0xff, '\n'),
		"oversized line":         []byte(valid + "ipfs " + strings.Repeat("a", 4096) + "\n"),
	}
	for name, commands := range tests {
		t.Run(name, func(t *testing.T) {
			server := compatibilityServer(t, commands)
			defer server.Close()
			_, err := newClient(t, server.URL, nil).CheckCompatibility(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) || protocol.Endpoint != "commands" {
				t.Fatalf("error = %T %v, want commands ProtocolError", err, err)
			}
		})
	}
}

func TestCompatibilityIgnoresMalformedShorthandAlias(t *testing.T) {
	commands := strings.Join(compatibleCommandTree, "\n") +
		"\nipfs object patch add-link --allow-non-unixfs / ipfs object patch add-link --\n"
	server := compatibilityServer(t, []byte(commands))
	defer server.Close()
	if _, err := newClient(t, server.URL, nil).CheckCompatibility(t.Context()); err != nil {
		t.Fatalf("stock-Kubo-style malformed shorthand alias was not ignored: %v", err)
	}
}

func TestCompatibilityRejectsDuplicateVersionField(t *testing.T) {
	for name, body := range map[string]string{
		"exact":        `{"Version":"0.43.0","Version":"0.42.0"}`,
		"case folded":  `{"Version":"0.43.0","version":"0.42.0"}`,
		"noncanonical": `{"version":"0.42.0"}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			_, err := newClient(t, server.URL, nil).CheckCompatibility(t.Context())
			var protocol *kubo.ProtocolError
			if !errors.As(err, &protocol) {
				t.Fatalf("error = %T %v, want ProtocolError", err, err)
			}
		})
	}
}

func TestCompatibilityErrorRedactsBearer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]string{"Version": "0.42.0-" + testToken})
	}))
	defer server.Close()
	_, err := newClient(t, server.URL, nil).CheckCompatibility(t.Context())
	var compatibility *kubo.CompatibilityError
	if !errors.As(err, &compatibility) {
		t.Fatalf("error = %T %v, want CompatibilityError", err, err)
	}
	if strings.Contains(err.Error(), testToken) || strings.Contains(compatibility.Version, testToken) {
		t.Fatalf("compatibility error leaked bearer: %v", err)
	}
}

func TestNotPinnedClassificationPrecedesBearerRedaction(t *testing.T) {
	target := testBlock(t, cid.Raw, "not pinned target").Cid()
	for name, test := range map[string]struct {
		message string
		token   string
	}{
		"absent":             {message: "CID is not pinned", token: "not"},
		"recursive mismatch": {message: target.String() + " is pinned recursively", token: "recursively"},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				writeJSON(t, w, map[string]string{"Message": test.message})
			}))
			defer server.Close()
			client := newClient(t, server.URL, func(c *kubo.Config) { c.BearerToken = test.token })
			err := client.PinRemove(t.Context(), target, kubo.PinTypeDirect)
			if !errors.Is(err, kubo.ErrNotPinned) {
				t.Fatalf("PinRemove error = %v, want ErrNotPinned", err)
			}
		})
	}
}

func TestStreamConfigurationBounds(t *testing.T) {
	for name, configure := range map[string]func(*kubo.Config){
		"negative bytes": func(c *kubo.Config) { c.MaxStreamBytes = -1 },
		"excess bytes":   func(c *kubo.Config) { c.MaxStreamBytes = kubo.MaximumStreamBytes + 1 },
		"negative items": func(c *kubo.Config) { c.MaxStreamItems = -1 },
		"excess items":   func(c *kubo.Config) { c.MaxStreamItems = kubo.MaximumStreamItems + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := kubo.Config{BaseURL: "https://example.test", BearerToken: testToken}
			configure(&cfg)
			if _, err := kubo.New(cfg); err == nil {
				t.Fatal("New succeeded")
			}
		})
	}
	if _, err := kubo.New(kubo.Config{
		BaseURL:        "https://example.test",
		BearerToken:    testToken,
		MaxStreamBytes: kubo.DefaultMaxStreamBytes + 1,
		MaxStreamItems: kubo.DefaultMaxStreamItems + 1,
	}); err != nil {
		t.Fatalf("New with explicitly raised finite stream budgets: %v", err)
	}
}
