# Kestra Dropbox Plugin

## What

- Provides plugin components under `io.kestra.plugin.dropbox`.
- Includes classes such as `DropboxFile`, `Delete`, `Upload`, `List`.

## Why

- What user problem does this solve? Teams need to connect Kestra workflows to Dropbox from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Dropbox steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Dropbox.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `dropbox`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.dropbox.files.Copy`
- `io.kestra.plugin.dropbox.files.CreateFolder`
- `io.kestra.plugin.dropbox.files.Delete`
- `io.kestra.plugin.dropbox.files.Download`
- `io.kestra.plugin.dropbox.files.GetMetadata`
- `io.kestra.plugin.dropbox.files.List`
- `io.kestra.plugin.dropbox.files.Move`
- `io.kestra.plugin.dropbox.files.Search`
- `io.kestra.plugin.dropbox.files.Upload`

### Project Structure

```
plugin-dropbox/
├── src/main/java/io/kestra/plugin/dropbox/models/
├── src/test/java/io/kestra/plugin/dropbox/models/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
