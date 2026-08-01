package quickstart

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

type renderedCompose struct {
	Services map[string]renderedService `json:"services"`
	Networks map[string]renderedNetwork `json:"networks"`
}

type renderedService struct {
	CapAdd        []string            `json:"cap_add"`
	CapDrop       []string            `json:"cap_drop"`
	CPUs          float64             `json:"cpus"`
	ContainerName string              `json:"container_name"`
	ExtraHosts    []string            `json:"extra_hosts"`
	Healthcheck   renderedHealthcheck `json:"healthcheck"`
	Init          bool                `json:"init"`
	Logging       renderedLogging     `json:"logging"`
	MemLimit      string              `json:"mem_limit"`
	NetworkMode   string              `json:"network_mode"`
	Networks      map[string]any      `json:"networks"`
	PidsLimit     int                 `json:"pids_limit"`
	Ports         []renderedPort      `json:"ports"`
	Privileged    bool                `json:"privileged"`
	ReadOnly      bool                `json:"read_only"`
	SecurityOpt   []string            `json:"security_opt"`
	Tmpfs         []string            `json:"tmpfs"`
	User          string              `json:"user"`
	Volumes       []renderedVolume    `json:"volumes"`
}

type renderedHealthcheck struct {
	Test []string `json:"test"`
}

type renderedLogging struct {
	Driver  string            `json:"driver"`
	Options map[string]string `json:"options"`
}

type renderedPort struct {
	HostIP   string `json:"host_ip"`
	Mode     string `json:"mode"`
	Protocol string `json:"protocol"`
	Target   int    `json:"target"`
}

type renderedVolume struct {
	Type     string `json:"type"`
	Target   string `json:"target"`
	ReadOnly bool   `json:"read_only"`
}

type renderedNetwork struct {
	External bool `json:"external"`
}

func TestDistributedQuickstartsRenderLeastPrivilege(t *testing.T) {
	for _, fixture := range []struct {
		dir              string
		service          string
		publishedTargets map[int]string
		writableTarget   string
		unusedVolumePath string
	}{
		{"follower", "follower", map[int]string{8550: "127.0.0.1", 9550: "127.0.0.1", 4001: ""}, "/var/lib/bloar", "/var/lib/bloar-kubo-replica"},
		{"kubo-replica", "replica", map[int]string{8550: "127.0.0.1", 9097: "127.0.0.1"}, "/var/lib/bloar-kubo-replica", "/var/lib/bloar"},
	} {
		t.Run(fixture.dir, func(t *testing.T) {
			rendered := renderCompose(t, fixture.dir)
			service, ok := rendered.Services[fixture.service]
			if !ok || len(rendered.Services) != 1 {
				t.Fatalf("services = %v, want only %q", rendered.Services, fixture.service)
			}
			if service.NetworkMode == "host" || service.ContainerName != "" || service.Privileged || len(service.CapAdd) != 0 {
				t.Fatalf("unsafe namespace/runtime settings: network_mode=%q container_name=%q privileged=%t cap_add=%v",
					service.NetworkMode, service.ContainerName, service.Privileged, service.CapAdd)
			}
			if service.User != "65532:65532" || !service.Init || !service.ReadOnly ||
				!slices.Contains(service.CapDrop, "ALL") ||
				!slices.Contains(service.SecurityOpt, "no-new-privileges:true") {
				t.Fatalf("least-privilege settings missing: user=%q init=%t read_only=%t cap_drop=%v security_opt=%v",
					service.User, service.Init, service.ReadOnly, service.CapDrop, service.SecurityOpt)
			}
			if service.CPUs <= 0 || service.MemLimit == "" || service.MemLimit == "0" || service.PidsLimit <= 0 {
				t.Fatalf("resource bounds = cpus %v memory %s pids %d", service.CPUs, service.MemLimit, service.PidsLimit)
			}
			if !slices.Contains(service.Healthcheck.Test, "-healthcheck") {
				t.Fatalf("healthcheck does not invoke the readiness mode: %v", service.Healthcheck.Test)
			}
			if service.Logging.Driver != "json-file" || service.Logging.Options["max-size"] == "" ||
				service.Logging.Options["max-file"] == "" {
				t.Fatalf("bounded logging missing: %+v", service.Logging)
			}
			if len(service.Ports) != len(fixture.publishedTargets) {
				t.Fatalf("ports = %+v, want targets %v", service.Ports, fixture.publishedTargets)
			}
			seenTargets := make(map[int]bool, len(service.Ports))
			for _, port := range service.Ports {
				hostIP, ok := fixture.publishedTargets[port.Target]
				if !ok || port.HostIP != hostIP || port.Protocol != "tcp" || port.Mode != "ingress" ||
					seenTargets[port.Target] {
					t.Fatalf("unexpected published port: %+v", port)
				}
				seenTargets[port.Target] = true
			}
			assertMissingBindPathsFail(t, fixture.dir, len(service.Volumes))
			assertMounts(t, service, fixture.writableTarget, fixture.unusedVolumePath)
		})
	}
}

func TestKuboQuickstartUsesOnlyAuthenticatedControlBridge(t *testing.T) {
	rendered := renderCompose(t, "kubo-replica")
	service := rendered.Services["replica"]
	if _, ok := service.Networks["kubo-control"]; !ok {
		t.Fatal("replica is not attached to kubo-control")
	}
	if !rendered.Networks["kubo-control"].External {
		t.Fatal("kubo-control is not an external persistent network")
	}
	if !slices.Contains(service.ExtraHosts, "kubo-api=172.30.189.1") {
		t.Fatalf("Kubo bridge gateway mapping missing: %v", service.ExtraHosts)
	}
	assertDistributedKuboTokenCannotMutateConfig(t)
}

func assertDistributedKuboTokenCannotMutateConfig(t *testing.T) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate quickstart fixtures")
	}
	dir := filepath.Join(filepath.Dir(file), "kubo-replica")
	config, err := os.ReadFile(filepath.Join(dir, "kubo-replica.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(config), "provider_policy_check: external") {
		t.Fatal("distributed Kubo config does not declare the host-owned provider-policy check")
	}
	readme, err := os.ReadFile(filepath.Join(dir, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	const prefix = "KUBO_ALLOWED_PATHS='"
	parts := strings.SplitN(string(readme), prefix, 2)
	if len(parts) != 2 {
		t.Fatal("distributed Kubo RPC allowlist is absent")
	}
	allowlist, _, ok := strings.Cut(parts[1], "'")
	if !ok {
		t.Fatal("distributed Kubo RPC allowlist is unterminated")
	}
	var got []string
	if err := json.Unmarshal([]byte(allowlist), &got); err != nil {
		t.Fatalf("distributed Kubo RPC allowlist is not JSON: %v", err)
	}
	want := []string{
		"/api/v0/version",
		"/api/v0/commands",
		"/api/v0/id",
		"/api/v0/block/get",
		"/api/v0/block/put",
		"/api/v0/pin/add",
		"/api/v0/pin/ls",
		"/api/v0/pin/rm",
		"/api/v0/pin/update",
		"/api/v0/provide/once",
		"/api/v0/routing/get",
		"/api/v0/swarm/connect",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("distributed Kubo RPC allowlist = %v, want exact runtime contract %v", got, want)
	}
}

func assertMounts(t *testing.T, service renderedService, writable, unused string) {
	t.Helper()
	foundWritable := false
	for _, volume := range service.Volumes {
		if volume.Type != "bind" {
			t.Fatalf("unexpected non-bind durable/config mount: %+v", volume)
		}
		if volume.Target == writable {
			foundWritable = !volume.ReadOnly
		} else if !volume.ReadOnly {
			t.Fatalf("unexpected writable bind mount: %+v", volume)
		}
	}
	if !foundWritable {
		t.Fatalf("durable state %s is not the one writable bind", writable)
	}
	if !slices.ContainsFunc(service.Tmpfs, func(value string) bool {
		return strings.HasPrefix(value, unused+":")
	}) {
		t.Fatalf("unused image volume %s is not explicitly shadowed: %v", unused, service.Tmpfs)
	}
}

func assertMissingBindPathsFail(t *testing.T, fixture string, want int) {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate quickstart fixtures")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(file), fixture, "compose.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(raw), "create_host_path: false"); got != want {
		t.Fatalf("create_host_path:false count = %d, want one for each of %d bind mounts", got, want)
	}
}

func renderCompose(t *testing.T, fixture string) renderedCompose {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI is unavailable")
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate quickstart fixtures")
	}
	command := exec.Command("docker", "compose", "config", "--format", "json")
	command.Dir = filepath.Join(filepath.Dir(file), fixture)
	// The samples deliberately reject a missing verified release digest. Tests
	// satisfy that independent gate before inspecting the rendered sandbox.
	command.Env = append(os.Environ(),
		"BLOAR_IMAGE_DIGEST=sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	raw, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose config: %v\n%s", err, raw)
	}
	var rendered renderedCompose
	if err := json.Unmarshal(raw, &rendered); err != nil {
		t.Fatalf("decoding rendered Compose: %v", err)
	}
	return rendered
}
