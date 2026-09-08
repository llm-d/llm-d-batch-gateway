/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package migrate

import (
	"context"
	"flag"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

// Subcommand is the argv[1] value that dispatches a binary into RunCommand.
const Subcommand = "migrate"

// RunCommand is the `migrate` subcommand shared by all binaries. It connects
// to PostgreSQL, applies pending migrations, and returns. The connection URL
// comes from --postgresql-url or, when unset, the mounted app secret.
func RunCommand(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet(Subcommand, flag.ContinueOnError)
	url := fs.String("postgresql-url", "", "PostgreSQL connection URL (default: "+ucom.SecretKeyPostgreSQLURL+" from the mounted app secret)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *url == "" {
		secretURL, err := ucom.ReadSecretFile(ucom.SecretKeyPostgreSQLURL)
		if err != nil {
			return fmt.Errorf("read %s secret: %w", ucom.SecretKeyPostgreSQLURL, err)
		}
		if secretURL == "" {
			return fmt.Errorf("no PostgreSQL URL: pass --postgresql-url or mount the %s secret", ucom.SecretKeyPostgreSQLURL)
		}
		*url = secretURL
	}

	pool, err := pgxpool.New(ctx, *url)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("applying schema migrations")

	applied, err := Run(ctx, pool)
	if err != nil {
		return err
	}
	for _, m := range applied {
		logger.Info("applied migration", "version", m.Version, "name", m.Name)
	}

	version, err := LatestVersion()
	if err != nil {
		return err
	}
	logger.Info("schema migrations complete", "applied", len(applied), "version", version)
	return nil
}
