# Main-branch test gate

Before merging or pushing changes to `main`, run the upstream non-clustered
CI test shards and the fork's relevant tests, including real object-store
integration tests for S3 storage changes. Use `scripts/runTestsOnTravis.sh` and
`.github/workflows/tests.yaml` for the upstream shard definitions. Do not treat
focused tests, mocks, or a build as completion of this gate. Record failures
and resolve them before merging; do not silently skip a failing test.

Keep generated reports, benchmark output, and temporary experiment artifacts
out of commits. Commit maintained source, tests, configuration, and documentation.
