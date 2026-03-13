# Kestra Dropbox Plugin

## What

description = 'Dropbox Plugin for Kestra Exposes 9 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Dropbox, allowing orchestration of Dropbox-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
