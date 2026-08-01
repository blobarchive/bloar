// Command bloar-swarm-inspect runs one bounded, local-view swarm census.
// Transport implementations are deliberately injected: the command's common
// layer owns limits and output policy, while an adapter must provide
// peer-targeted proofs that cannot be satisfied by an arbitrary Bitswap peer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ipfs/go-cid"

	"github.com/blobarchive/bloar/census"
	"github.com/blobarchive/bloar/p2p"
)

type transport struct {
	census.Finder
	census.Prober
	Close func() error
}

type transportFactory interface {
	RegisterFlags(*flag.FlagSet)
	Open(context.Context, census.Limits) (transport, error)
}

// A transport adapter in this package replaces configuredTransport from init.
// Keeping the unavailable default explicit prevents a generic Bitswap fetch
// from being mistaken for proof that one particular provider served a block.
var configuredTransport transportFactory = unavailableTransport{}

type unavailableTransport struct{}

func (unavailableTransport) RegisterFlags(*flag.FlagSet) {}

func (unavailableTransport) Open(context.Context, census.Limits) (transport, error) {
	return transport{}, errors.New("no peer-targeted census transport is compiled into this build")
}

type repeatedStrings []string

func (values *repeatedStrings) String() string { return strings.Join(*values, ",") }

func (values *repeatedStrings) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("value must not be empty")
	}
	*values = append(*values, value)
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr, configuredTransport); err != nil {
		fmt.Fprintf(os.Stderr, "bloar-swarm-inspect: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, factory transportFactory) (runErr error) {
	flags := flag.NewFlagSet("bloar-swarm-inspect", flag.ContinueOnError)
	flags.SetOutput(stderr)
	rendezvousValue := flags.String("rendezvous", "", "rendezvous CID to query")
	networkValue := flags.String("net", "", "Bloar network used to derive the rendezvous CID with -head")
	headValue := flags.String("head", "", "Bloar head used to derive the rendezvous CID with -net")
	currentValue := flags.String("current", "", "current authenticated head-root or manifest CID to challenge")
	var historicalValues repeatedStrings
	flags.Var(&historicalValues, "historical", "historical challenge CID (repeat for multiple archive-depth samples)")
	format := flags.String("format", "json", "output format: json or prometheus")
	rawPeers := flags.Bool("raw-peers", false, "include peer IDs, admitted addresses, and per-peer results in JSON")
	pretty := flags.Bool("pretty", false, "indent JSON output")

	limits := census.Limits{}
	flags.IntVar(&limits.MaxProviders, "max-providers", census.DefaultMaxProviders, "maximum deduplicated providers sampled")
	flags.IntVar(&limits.MaxAddressBytes, "max-address-bytes", census.DefaultMaxAddressBytes, "maximum total multiaddr wire bytes admitted")
	flags.IntVar(&limits.Concurrency, "concurrency", census.DefaultConcurrency, "maximum simultaneous peer probes")
	flags.IntVar(&limits.MaxHistorical, "max-historical", census.DefaultMaxHistorical, "maximum historical challenge CIDs")
	flags.DurationVar(&limits.OverallTimeout, "timeout", census.DefaultOverallTimeout, "overall census deadline")
	flags.DurationVar(&limits.DiscoveryTimeout, "discovery-timeout", census.DefaultDiscoveryTimeout, "provider discovery deadline")
	flags.DurationVar(&limits.ProbeTimeout, "probe-timeout", census.DefaultProbeTimeout, "deadline for one peer probe")
	if factory == nil {
		return errors.New("transport factory is nil")
	}
	factory.RegisterFlags(flags)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if *format != "json" && *format != "prometheus" {
		return fmt.Errorf("-format must be json or prometheus, got %q", *format)
	}
	if *rawPeers && *format != "json" {
		return errors.New("-raw-peers is available only with -format=json")
	}
	if *pretty && *format != "json" {
		return errors.New("-pretty is available only with -format=json")
	}
	var (
		rendezvous cid.Cid
		err        error
	)
	rendezvousText := strings.TrimSpace(*rendezvousValue)
	network := strings.TrimSpace(*networkValue)
	head := strings.TrimSpace(*headValue)
	switch {
	case rendezvousText != "" && (network != "" || head != ""):
		return errors.New("use either -rendezvous or the -net/-head pair, not both")
	case rendezvousText != "":
		rendezvous, err = cid.Parse(rendezvousText)
		if err != nil {
			return fmt.Errorf("parsing -rendezvous: %w", err)
		}
	case network != "" && head != "":
		rendezvous, err = p2p.RendezvousCID(network, head)
		if err != nil {
			return fmt.Errorf("deriving rendezvous CID: %w", err)
		}
	default:
		return errors.New("provide either -rendezvous or both -net and -head")
	}
	current, err := cid.Parse(strings.TrimSpace(*currentValue))
	if err != nil {
		return fmt.Errorf("parsing -current: %w", err)
	}
	historical := make([]cid.Cid, 0, len(historicalValues))
	for index, value := range historicalValues {
		challenge, parseErr := cid.Parse(value)
		if parseErr != nil {
			return fmt.Errorf("parsing -historical value %d: %w", index+1, parseErr)
		}
		historical = append(historical, challenge)
	}
	if len(historical) == 0 {
		return errors.New("at least one -historical challenge is required")
	}
	if err := census.ValidateInputs(rendezvous, current, historical, limits); err != nil {
		return err
	}

	opened, err := factory.Open(ctx, limits)
	if err != nil {
		return fmt.Errorf("opening census transport: %w", err)
	}
	if opened.Close != nil {
		defer func() { runErr = errors.Join(runErr, opened.Close()) }()
	}
	inspector, err := census.New(census.Config{
		Rendezvous:   rendezvous,
		Current:      current,
		Historical:   historical,
		Finder:       opened.Finder,
		Prober:       opened.Prober,
		Limits:       limits,
		IncludePeers: *rawPeers,
	})
	if err != nil {
		return err
	}
	report := inspector.Inspect(ctx)
	switch *format {
	case "json":
		if err := census.WriteJSON(stdout, report, *pretty); err != nil {
			return fmt.Errorf("writing JSON report: %w", err)
		}
	case "prometheus":
		if _, err := io.WriteString(stdout, report.Prometheus()); err != nil {
			return fmt.Errorf("writing Prometheus report: %w", err)
		}
	}
	if !report.Complete {
		return fmt.Errorf("census incomplete (observed=%d probed=%d errors=%d timed_out=%t)",
			report.LowerBounds.Observed, report.ProbeCompleted, report.ErrorCount, report.TimedOut)
	}
	return nil
}
