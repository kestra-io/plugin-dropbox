# How to use the Dropbox plugin

Upload, download, search, and manage files and folders in Dropbox from Kestra flows.

## Authentication

Set `accessToken` to your Dropbox access token (required on every task). Store it in a [secret](https://kestra.io/docs/concepts/secret) and apply it globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`files.Upload` writes a file to Dropbox — set `from` (a `kestra://` URI) and `to` (the destination Dropbox path). Set `mode` to `ADD` (default) or `OVERWRITE`. Set `autorename: true` to avoid conflicts.

`files.Download` retrieves a file — set `from` to the Dropbox path.

`files.List` lists a folder — set `from` to the folder path. Set `recursive: true` to traverse subfolders. Control result handling with `fetchType` (default `FETCH`) and bound with `limit` (default 2000).

`files.Search` searches for files — set `query` (required). Optionally scope to a `path`, filter by `fileExtensions`, and cap results with `maxResults`.

`files.Copy` copies a file or folder — set `from` and `to`. Set `autorename: true` to rename on conflict.

`files.Move` moves a file or folder — set `from` and `to`. Set `autorename: true` to rename on conflict and `allowOwnershipTransfer: true` to move across owners.

`files.Delete` removes a file or folder — set `from`.

`files.CreateFolder` creates a folder — set `path`. Set `autorename: true` to avoid conflicts.

`files.GetMetadata` retrieves metadata for a file or folder — set `path`.
