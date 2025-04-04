/*
Copyright (c) 2025 Red Hat Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
language governing permissions and limitations under the License.
*/

package cmd

import (
	"log/slog"
	"runtime/debug"

	"github.com/spf13/cobra"

	"github.com/innabox/fulfillment-service/internal"
)

// NewVersionCommand creates and returns the `version` command.
func NewVersionCommand() *cobra.Command {
	runner := &versionCommandRunner{}
	command := &cobra.Command{
		Use:   "version",
		Short: "Prints version information",
		Args:  cobra.NoArgs,
		RunE:  runner.run,
	}
	return command
}

// versionCommandRunner contains the data and logic needed to run the `version` command.
type versionCommandRunner struct {
}

// run executes the `version` command.
func (c *versionCommandRunner) run(cmd *cobra.Command, argv []string) error {
	// Get the context:
	ctx := cmd.Context()

	// Get the logger:
	logger := internal.LoggerFromContext(ctx)

	// Calculate the values:
	buildCommit := unknownSettingValue
	buildTime := unknownSettingValue
	info, ok := debug.ReadBuildInfo()
	if ok {
		vcsRevision := c.getSetting(info, vcsRevisionSettingKey)
		if vcsRevision != "" {
			buildCommit = vcsRevision
		}
		vcsTime := c.getSetting(info, vcsTimeSettingKey)
		if vcsTime != "" {
			buildTime = vcsTime
		}
	}

	// Print the values:
	logger.Info(
		"Version",
		slog.String("commit", buildCommit),
		slog.String("time", buildTime),
	)

	return nil
}

// getSetting returns the value of the build setting witht he given key. Returns an empty string
// if no such setting exists.
func (c *versionCommandRunner) getSetting(info *debug.BuildInfo, key string) string {
	for _, s := range info.Settings {
		if s.Key == key {
			return s.Value
		}
	}
	return ""
}

// Names of build settings we are interested on:
const (
	vcsRevisionSettingKey = "vcs.revision"
	vcsTimeSettingKey     = "vcs.time"
)

// Fallback value for unknown settings:
const unknownSettingValue = "unknown"
