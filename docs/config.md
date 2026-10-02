## TestRift.NUnit configuration (`TestRiftNUnit.yaml`)

`TestRift.NUnit` reads an optional YAML config file to configure:

- Auto-starting the server for local runs (`autoStartServer`)
- Server connection (`serverUrl`)
- Optional ingest authentication (`ingestToken`)
- Run naming (`runName`, optional `runId`)
- Run metadata (`metadata`)
- Target Run context (`target`, `purpose`, `sources`, optional `parentRunId`)
- Optional URL file generation for CI (`urlFiles`)

### How the config file is discovered

At test run start, the plugin loads config from the first match below:

- **1)** `TESTRIFT_NUNIT_YAML` (filesystem path to the YAML file)
- **2)** `./TestRiftNUnit.yaml` in the **current working directory**

If no config file is found, the plugin uses defaults (server URL defaults to `http://localhost:8080`).

### Environment variable expansion

All string fields support `${env:VAR_NAME}` expansion. A referenced variable must be set; missing variables fail configuration loading, which disables reporting for the run (see below).

Example:

```yaml
runName: CI run ${env:GITHUB_RUN_NUMBER}
```

### When the server is unavailable

Reporting never blocks the tests. If the config cannot be loaded, the server cannot be reached within 10 seconds, or the server rejects `run_started`, the tests run without reporting and a `WARNING:` line is printed to the NUnit progress output. Sends and closes are bounded by timeouts, so a server that stops responding mid-run cannot stall the tests.

At the end of the run, a warning reports how many events were lost if the connection dropped, and whether the KPI upload failed. Lost events are not replayed; the server marks such runs as aborted.

### Schema

#### `serverUrl` (optional)

Base URL to the TestRift server (used for WebSocket + HTTP). The `TESTRIFT_SERVER_URL` environment variable overrides this value.

Example: `http://localhost:8080`

#### `ingestToken` (optional)

Shared secret sent as `X-TestRift-Ingest-Token` with the NUnit WebSocket connection and HTTP uploads such as KPIs and attachments. Prefer setting the `TESTRIFT_INGEST_TOKEN` environment variable, which overrides this value, instead of storing the token in YAML.

The server must be configured with the same `auth.ingest_token`. HTTPS is required when the server's ingest TLS mode is enabled.

#### `autoStartServer` (optional)

Configuration block for auto-starting TestRift Server automatically.

- This only runs when a YAML config file is loaded (i.e. `TestRiftNUnit.yaml` exists or `TESTRIFT_NUNIT_YAML` is set).
- This only supports **local** server URLs (`http://localhost:...`, `http://127.0.0.1:...`, `http://[::1]:...`).
- It assumes the server is installed via pip and available as the `testrift-server` command on `PATH`.
- When enabled, the plugin always attempts to start `testrift-server` (even if `/health` is already OK) so that server-side config mismatch checks are enforced.
- The server compares configs by their **effective values** (config hash), not by filename/path. If your YAML only overrides defaults, it may still be considered identical to the running server’s config.

Example:

```yaml
autoStartServer:
  enabled: true
  serverYaml: ./testrift_server.yaml
  restartOnConfigChange: false
serverUrl: http://localhost:8080
```

Fields:

##### `autoStartServer.enabled` (optional)

If `true`, enables auto-start.

##### `autoStartServer.serverYaml` (optional)

If set, this path will be passed to the server via the `TESTRIFT_SERVER_YAML` environment variable when starting it.

- Supports `${env:VAR_NAME}` expansion.
- The path can be relative; it will be interpreted relative to the directory containing `TestRiftNUnit.yaml`.
- If set and the file does not exist, the test run will fail fast with an error.
- If you run via `dotnet test` and your `TestRiftNUnit.yaml` is copied to the test output directory (`bin/...`), make sure the server YAML is copied there too (or use an absolute path).
- Windows note: avoid YAML double-quoted paths like `"C:\test\TestRiftServer.yaml"` (because `\t` becomes a TAB). Prefer:
  - `serverYaml: C:\test\TestRiftServer.yaml`, or
  - `serverYaml: 'C:\test\TestRiftServer.yaml'` (single quotes), or
  - `serverYaml: C:/test/TestRiftServer.yaml` (forward slashes), or
  - `serverYaml: "C:\\test\\TestRiftServer.yaml"` (escaped backslashes)

##### `autoStartServer.restartOnConfigChange` (optional)

If `true`, starts the server with `--restart-on-config`. If a server is already running on the port with a different config, it will be shut down and restarted with the new config.

#### `runName` (optional)

Human-readable name shown in the UI.

#### `runId` (optional)

Custom run ID sent in `run_started`.

- The server validates that the run ID is URL-safe and not already in use.
- If the server rejects the run ID, the test run is aborted (the plugin treats it as a startup error).
- This is useful in CI when you want a predictable URL.

Given:

- `serverUrl: http://localhost:8080`
- `runId: my-run-123`

- `http://localhost:8080/testRun/my-run-123/index.html`

#### Environment variable: `TESTRIFT_PREPARED_RUN_ID`

Activate a prepared run instead of starting a new one.

- When the `TESTRIFT_PREPARED_RUN_ID` environment variable is set, the plugin sends `run_started` with this run ID to activate a run that was prepared by `testrift-collector`.
- The prepared run must exist on the server (prepared within the last 15 minutes).
- This is **not a YAML config field** - it's only read from the environment variable.

Example workflow:

```bash
# testrift-collector prepares the run and prints the run_id
RUN_ID=$(testrift-collector -c TestRiftNUnit.yaml)

# Pass the run_id to NUnit via environment variable
export TESTRIFT_PREPARED_RUN_ID=$RUN_ID
dotnet test
```

#### `metadata` (optional)

List of name/value/url entries shown in the UI:

```yaml
metadata:
  - name: Firmware
    value: ${env:FIRMWARE_BRANCH}
    url: https://example.com/builds/${env:BUILD_ID}
```

#### Target Run context (required)

Every direct Run requires a canonical Target, a purpose, and at least one source snapshot. Source branches and revisions are exact values used for Summary profile selection.

```yaml
target: nora-b26x
purpose: nightly
sources:
  firmware:
    branch: main
    revision: ${env:FIRMWARE_REVISION}
  test-system:
    branch: development
    revision: ${env:TEST_SYSTEM_REVISION}
```

`purpose` is one of `nightly`, `release`, `feature`, `manual`, `sanity`, or `rerun`. `parentRunId` is required only when `purpose: rerun` and rejected for every other purpose. `TESTRIFT_TARGET` overrides the YAML Target.

#### `urlFiles` (optional)

Write URLs to files after the run starts (handy in CI):

```yaml
urlFiles:
  runUrlFile: test_run_url.txt
  targetUrlFile: test_target_url.txt
```

Behavior:

- Files are written after the server replies to `run_started`.
- Paths support `${env:VAR_NAME}` expansion.
- `runUrlFile` is written if configured.
- `targetUrlFile` is written when the server returns a Target URL.
- URL-file contents are absolute URLs built from `serverUrl` plus the returned relative URL.

This is useful in CI to publish a link to results without parsing logs.

#### `collector` (optional)

Configuration for `testrift-collector` to collect git diffs between test runs.

This section is read by `testrift-collector` (not by the NUnit plugin) but is placed in the same YAML file for convenience.

```yaml
collector:
  repos:
    - name: my-app
      url: https://github.com/org/my-app
      path: ../my-app
    - name: firmware
      url: https://github.com/org/firmware
      gitUrl: https://github.com/org/firmware.git
      ref: ${env:FIRMWARE_SHA}
```

##### `collector.repos` (required)

List of repositories to collect diffs from.

Each repo entry has the following fields:

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique name for the repository (used as key in diff storage) |
| `url` | No | User-facing URL for linking in the UI |
| `path` | Conditional | Local filesystem path to the git repository |
| `gitUrl` | Conditional | Git remote URL (use with `ref`) |
| `ref` | Conditional | Branch, tag, or SHA (required when using `gitUrl`) |

**Note:** Each repo must specify either `path` OR `gitUrl` + `ref`, not both.

All fields support `${env:VAR_NAME}` expansion.

See [`testrift-collector` documentation](../../testrift-collector/README.md) for more details.

### Protocol mapping

This YAML config influences fields in the `run_started` message:

- `runName` → `run_name`
- `runId` → `run_id` (optional)
- `metadata` → `user_metadata`
- `target` → `target_key`
- `purpose` → `purpose`
- `parentRunId` → `parent_run_id` (reruns only)
- `sources` → `sources`

For the complete wire format, see [`websocket_protocol.md`](../../testrift-server/docs/websocket_protocol.md).

### Full example

```yaml
serverUrl: http://localhost:8080
runName: CI run ${env:GITHUB_RUN_NUMBER}
runId: ${env:GITHUB_RUN_ID}

metadata:
  - name: Firmware
    value: ${env:FIRMWARE_BRANCH}

target: nora-b26x
purpose: nightly
sources:
  firmware:
    branch: main
    revision: ${env:FIRMWARE_REVISION}

urlFiles:
  runUrlFile: test_run_url.txt
  targetUrlFile: test_target_url.txt
```


