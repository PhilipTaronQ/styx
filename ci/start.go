package ci

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

type (
	StartConfig struct {
		TemporalParams string
		Args           CiArgs
	}
)

func Start(ctx context.Context, cfg StartConfig) error {
	// there's no default deployment to build from or copy to
	for _, f := range []struct{ flag, val string }{
		{"styx_repo", cfg.Args.StyxRepo.Repo},
		{"copy_dest", cfg.Args.CopyDest},
		{"manifest_upstream", cfg.Args.ManifestUpstream},
	} {
		if f.val == "" {
			return fmt.Errorf("--%s is required", f.flag)
		}
	}

	c, _, err := getTemporalClient(ctx, cfg.TemporalParams)
	if err != nil {
		return err
	}

	opts := client.StartWorkflowOptions{
		ID:        fmt.Sprintf("ci-%s-%s", cfg.Args.StyxRepo.Branch, cfg.Args.Channel),
		TaskQueue: taskQueue,
	}
	run, err := c.ExecuteWorkflow(context.Background(), opts, ci, &cfg.Args)
	if err != nil {
		return err
	}
	fmt.Println("workflow id:", run.GetID(), "run id:", run.GetRunID())
	return nil
}
