package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"

	"github.com/dnr/styx/common/cobrautil"
	"github.com/nix-community/go-nix/pkg/storepath"
	"github.com/spf13/cobra"
)

const (
	spinVersion  = "spin-1"
	doc          = "This file contains Nix pins using the 'spin' styx-based pinning tool. See https://github.com/dnr/styx/tree/main/cmd/spin"
	schemaUrl    = "TODO"
	pinsJsonName = "Pins.json"
	pinsNixName  = "Pins.nix"
)

//go:embed Pins.nix
var pinsNixCode []byte

type pinJson struct {
	Doc      string     `json:"$doc"`
	Schema   string     `json:"$schema"`
	StoreDir string     `json:"$storeDir"`
	Version  string     `json:"$version"`
	Pins     []*pinData `json:"pins"`
}

type pinData struct {
	Name          string `json:"name"`
	StorePathName string `json:"storePathName"`
	StorePathHash string `json:"storePathHash"`
	OutputHash    string `json:"outputHash"`
	OriginalUrl   string `json:"originalUrl"`
	ResolvedUrl   string `json:"resolvedUrl"`
}

func updatePinsNix() error {
	if have, err := os.ReadFile(pinsNixName); err == nil && bytes.Equal(have, pinsNixCode) {
		return nil
	}
	return writeFileAtomic(pinsNixName, pinsNixCode)
}

// writeFileAtomic writes data to a temporary file next to name and renames it over name, so
// that a failed or interrupted write leaves the old contents in place.
func writeFileAtomic(name string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(name), "."+filepath.Base(name)+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name()) // fails harmlessly after the rename
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	} else if err := f.Chmod(0o644); err != nil {
		f.Close()
		return err
	} else if err := f.Sync(); err != nil {
		f.Close()
		return err
	} else if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), name)
}

func loadOrCreatePinJson(c *cobra.Command) error {
	b, err := os.ReadFile(pinsJsonName)
	var j pinJson
	if err == nil {
		err := json.Unmarshal(b, &j)
		if err != nil {
			return err
		} else if j.StoreDir != storepath.StoreDir {
			return fmt.Errorf("mismatched store dir %q != %q", j.StoreDir, storepath.StoreDir)
		} else if j.Version != spinVersion {
			return fmt.Errorf("mismatched version %q != %q", j.Version, spinVersion)
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	j.Doc = doc
	j.Schema = schemaUrl
	j.StoreDir = storepath.StoreDir
	j.Version = spinVersion
	cobrautil.Store(c, &j)
	return nil
}

func savePinJson(j *pinJson) error {
	b, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(pinsJsonName, b)
}

// copied from daemon/proto.go TarballResp to avoid dependency
type tarballResp struct {
	ResolvedUrl   string `json:"resolvedUrl"`
	StorePathHash string `json:"storePathHash"`
	StorePathName string `json:"storePathName"`
	NarHash       string `json:"narHash"`
	NarHashAlgo   string `json:"narHashAlgo"`
}

func (r *tarballResp) outputHash() string { return r.NarHashAlgo + ":" + r.NarHash }

func styxTarball(ctx context.Context, url string) (*tarballResp, error) {
	log.Println("running: styx tarball --json", url)
	var out tarballResp
	b, err := exec.CommandContext(ctx, "styx", "tarball", "--json", url).Output()
	if err != nil {
		return nil, err
	} else if err = json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (d *pinData) update(ctx context.Context) error {
	out, err := styxTarball(ctx, d.OriginalUrl)
	if err != nil {
		return err
	}
	log.Println("resolved to", out.ResolvedUrl)
	log.Println("using name", out.StorePathName)
	d.ResolvedUrl = out.ResolvedUrl
	d.StorePathName = out.StorePathName
	d.StorePathHash = out.StorePathHash
	d.OutputHash = out.outputHash()
	return nil
}

func (d *pinData) refresh(ctx context.Context) error {
	out, err := styxTarball(ctx, d.ResolvedUrl)
	if err != nil {
		return err
	}
	// If the contents behind the resolved url changed, telling styx about them doesn't make
	// the pinned path substitutable.
	if got := out.outputHash(); got != d.OutputHash {
		return fmt.Errorf("pin %q: %s now has hash %s, pinned %s; use `spin update %s` to accept the change",
			d.Name, d.ResolvedUrl, got, d.OutputHash, d.Name)
	} else if out.StorePathHash != d.StorePathHash {
		log.Printf("warning: pin %q: %s now gives store path %s-%s, pinned %s-%s",
			d.Name, d.ResolvedUrl, out.StorePathHash, out.StorePathName, d.StorePathHash, d.StorePathName)
	}
	return nil
}

func withAllFlag(c *cobra.Command) *bool {
	return c.Flags().Bool("all", false, "apply to all pins")
}

func main() {
	root := cobrautil.Cmd(
		&cobra.Command{
			Use:   "spin",
			Short: "Spin - simple Nix pinning tool using Styx",
		},
		cobrautil.Cmd(
			&cobra.Command{
				Use:   "init",
				Short: "init or re-init spin",
				Args:  cobra.NoArgs,
			},
			loadOrCreatePinJson,
			savePinJson,
			updatePinsNix,
		),
		cobrautil.Cmd(
			&cobra.Command{
				Use:   "refresh [--all] <name> ...",
				Short: "tell Styx about pins again so that they can be substituted",
			},
			withAllFlag,
			loadOrCreatePinJson,
			func(ctx context.Context, args []string, all *bool, j *pinJson) error {
				for _, d := range j.Pins {
					if *all || slices.Contains(args, d.Name) {
						if err := d.refresh(ctx); err != nil {
							return err
						}
					}
				}
				return nil
			},
			savePinJson,
			updatePinsNix,
		),
		cobrautil.Cmd(
			&cobra.Command{
				Use:   "add <name> <url>",
				Short: "add new pin",
				Args:  cobra.ExactArgs(2),
			},
			loadOrCreatePinJson,
			func(ctx context.Context, args []string, j *pinJson) error {
				name, url := args[0], args[1]
				newData := &pinData{
					Name:        name,
					OriginalUrl: url,
				}
				i := slices.IndexFunc(j.Pins, func(d *pinData) bool { return d.Name == name })
				if i >= 0 {
					log.Printf("pin %q already exists, replacing\n", name)
					j.Pins[i] = newData
				} else {
					j.Pins = append(j.Pins, newData)
				}
				return newData.update(ctx)
			},
			savePinJson,
			updatePinsNix,
		),
		cobrautil.Cmd(
			&cobra.Command{
				Use:   "update [--all] <name> ...",
				Short: "update pin(s)",
			},
			withAllFlag,
			loadOrCreatePinJson,
			func(ctx context.Context, args []string, all *bool, j *pinJson) error {
				for _, d := range j.Pins {
					if *all || slices.Contains(args, d.Name) {
						if err := d.update(ctx); err != nil {
							return err
						}
					}
				}
				return nil
			},
			savePinJson,
			updatePinsNix,
		),
	)
	if err := root.Execute(); err != nil {
		log.Fatal(err)
	}
}
