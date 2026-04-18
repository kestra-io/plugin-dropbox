# Kestra Dropbox Plugin

## What

- Provides plugin components under `io.kestra.plugin.dropbox`.
- Includes classes such as `DropboxFile`, `Delete`, `Upload`, `List`.

## Why

- This plugin integrates Kestra with Dropbox Files.
- It provides tasks that manage files and folders in Dropbox.

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
