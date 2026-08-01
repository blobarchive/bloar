// Command bloard is the bloar archive daemon: the HTTP API of spec 7 over the
// head engine, the blob catalog, and the on-disk store.
//
// Usage:
//
//	bloard [run] -config <path>          serve (the default subcommand)
//	bloard rebuild -config <path> [-clear]   rebuild the blob catalog from the blockstore
//	bloard fsck -config <path> [-head H] [-repair]   validate pinned blocks against their CIDs
//	bloard put-block -config <path> -cid <cid> <file>   write a raw block, validated against its CID
//	bloard config-inspect -config <path>   validate and print expanded config/profile metadata without reading secrets
//	bloard conflicts status -config <path> [-head H] [-json]   inspect durable multi-writer conflict latches
//	bloard conflicts clear -config <path> -head H -evidence sha256:<64hex>   clear one exact investigated latch
//
// # What this phase wires
//
// The daemon is the writer role of spec 11.1: it runs the mutation engine,
// accepts ingest, publishes the heads document over HTTPS and -- when p2p is
// configured -- over IPNS (spec 8.1), serves its blocks to peers over bitswap
// (spec 11.2), and reconciles pins and collects garbage (spec 9). It does not
// follow other archives' heads; that is spec 11.3, and the follow config key is
// parsed and held rather than acted on (see Config).
//
// A config with no p2p block runs no libp2p at all, which is a supported
// deployment and not a half-built one: a writer reachable only over HTTPS.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		// The logger may not exist yet (a config that will not parse), so this
		// one failure path is the bare stream.
		fmt.Fprintf(os.Stderr, "bloard: %v\n", err)
		os.Exit(1)
	}
}

// run dispatches a subcommand. "run" is the default, so `bloard -config x` is
// `bloard run -config x`: serving is what this binary is for, and an operator's
// unit file should not have to say so.
func run(args []string, out io.Writer) error {
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "run":
		fs := flag.NewFlagSet("run", flag.ContinueOnError)
		config := fs.String("config", "", "path to the YAML config (spec 12)")
		healthcheck := fs.Bool("healthcheck", false, "check the configured readiness endpoint, then exit")
		if err := fs.Parse(args); err != nil {
			return err
		}
		cfg, err := loadFrom(*config)
		if err != nil {
			return err
		}
		if *healthcheck {
			return runReadinessHealthcheck(cfg.Server.MetricsListen)
		}
		ctx, stop := signalContext()
		defer stop()
		return serve(ctx, cfg)

	case "rebuild":
		fs := flag.NewFlagSet("rebuild", flag.ContinueOnError)
		config := fs.String("config", "", "path to the YAML config (spec 12)")
		clear := fs.Bool("clear", false, "delete every catalog entry before the walk, so the catalog ends up saying "+
			"nothing but what the blockstore holds")
		if err := fs.Parse(args); err != nil {
			return err
		}
		cfg, err := loadFrom(*config)
		if err != nil {
			return err
		}
		ctx, stop := signalContext()
		defer stop()
		return rebuild(ctx, cfg, *clear, out)

	case "fsck":
		fs := flag.NewFlagSet("fsck", flag.ContinueOnError)
		config := fs.String("config", "", "path to the YAML config (spec 12)")
		head := fs.String("head", "", "restrict the walk to one head; default is every head in the config plus staging")
		repair := fs.Bool("repair", false, "delete every corrupt block found, turning it into a clean miss to be refilled "+
			"(needs exclusive store ownership: the daemon must be stopped)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		cfg, err := loadFrom(*config)
		if err != nil {
			return err
		}
		ctx, stop := signalContext()
		defer stop()
		return fsck(ctx, cfg, *repair, *head, out)

	case "put-block":
		fs := flag.NewFlagSet("put-block", flag.ContinueOnError)
		config := fs.String("config", "", "path to the YAML config (spec 12)")
		cidStr := fs.String("cid", "", "the CID the file's bytes must reproduce exactly (required)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if *cidStr == "" {
			return errors.New("put-block: -cid is required")
		}
		if fs.NArg() != 1 {
			return errors.New("put-block: exactly one file argument is required")
		}
		cfg, err := loadFrom(*config)
		if err != nil {
			return err
		}
		ctx, stop := signalContext()
		defer stop()
		return putBlock(ctx, cfg, *cidStr, fs.Arg(0), out)

	case "config-inspect":
		fs := flag.NewFlagSet("config-inspect", flag.ContinueOnError)
		config := fs.String("config", "", "path to the YAML config (spec 12)")
		if err := fs.Parse(args); err != nil {
			return err
		}
		if *config == "" {
			return errors.New("-config is required")
		}
		return inspectConfig(*config, out)

	case "conflicts":
		return runConflicts(args, out)

	default:
		return fmt.Errorf("unknown subcommand %q; try `bloard run -config <path>`, `bloard rebuild -config <path>`, "+
			"`bloard fsck -config <path>`, `bloard put-block -config <path> -cid <cid> <file>`, "+
			"`bloard config-inspect -config <path>`, or `bloard conflicts status -config <path>`", cmd)
	}
}

// loadFrom loads the config named by a -config flag.
func loadFrom(path string) (*Config, error) {
	if path == "" {
		return nil, errors.New("-config is required")
	}
	return LoadConfig(path)
}

// signalContext returns a context cancelled by SIGINT or SIGTERM, and the stop
// that unregisters the handler. Unregistering re-arms Go's default, so a second
// SIGINT during a slow shutdown kills the process, which is what an operator
// sending it means.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

// newLogger returns the daemon's logger.
func newLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
