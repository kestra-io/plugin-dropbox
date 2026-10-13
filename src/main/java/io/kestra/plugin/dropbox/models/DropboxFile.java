package io.kestra.plugin.dropbox.models;

import java.util.Date;

import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.Metadata;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class DropboxFile {
    @Schema(title = "Item name")
    @PluginProperty(group = "advanced")
    private final String name;

    @Schema(title = "Lower-cased path", description = "Dropbox `path_lower` for the entry — the item's lower-cased path, not a stable unique ID.")
    @PluginProperty(group = "advanced")
    private final String id;

    @Schema(title = "Display path", description = "Case-preserved display path from Dropbox.")
    @PluginProperty(group = "advanced")
    private final String path;

    @Schema(title = "Entry type", description = "Either 'file' or 'folder'.")
    @PluginProperty(group = "advanced")
    private final String type;

    @Schema(title = "Size in bytes", description = "Null for folders.")
    @PluginProperty(group = "advanced")
    private final Long size;

    @Schema(title = "Client modified time", description = "Last modified timestamp from Dropbox. Null for folders.")
    @PluginProperty(group = "advanced")
    private final Date clientModified;

    @Schema(title = "Media info", description = "Photo or video metadata. Only populated by `GetMetadata` with `includeMediaInfo: true`.")
    @PluginProperty(group = "advanced")
    private final DropboxMediaInfo mediaInfo;

    public static DropboxFile of(Metadata metadata) {
        DropboxFileBuilder builder = DropboxFile.builder()
            .name(metadata.getName())
            .id(metadata.getPathLower())
            .path(metadata.getPathDisplay());

        if (metadata instanceof FileMetadata) {
            FileMetadata file = (FileMetadata) metadata;
            builder.type("file")
                .size(file.getSize())
                .clientModified(file.getClientModified())
                .mediaInfo(DropboxMediaInfo.of(file.getMediaInfo()));
        } else if (metadata instanceof FolderMetadata) {
            builder.type("folder");
        }

        return builder.build();
    }
}
