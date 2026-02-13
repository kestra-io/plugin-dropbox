package io.kestra.plugin.dropbox.models;

import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.FolderMetadata;
import com.dropbox.core.v2.files.Metadata;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.Date;

@Builder
@Getter
public class DropboxFile {
    @Schema(title = "Item name")
    private final String name;

    @Schema(title = "Unique item ID", description = "Dropbox path_lower for the entry.")
    private final String id;

    @Schema(title = "Display path", description = "Lower-case path for display.")
    private final String path;

    @Schema(title = "Entry type", description = "Either 'file' or 'folder'.")
    private final String type;

    @Schema(title = "Size in bytes", description = "Null for folders.")
    private final Long size;

    @Schema(title = "Client modified time", description = "Last modified timestamp from Dropbox.")
    private final Date clientModified;

    public static DropboxFile of(Metadata metadata) {
        DropboxFileBuilder builder = DropboxFile.builder()
            .name(metadata.getName())
            .id(metadata.getPathLower())
            .path(metadata.getPathDisplay());

        if (metadata instanceof FileMetadata) {
            FileMetadata file = (FileMetadata) metadata;
            builder.type("file")
                .size(file.getSize())
                .clientModified(file.getClientModified());
        } else if (metadata instanceof FolderMetadata) {
            builder.type("folder");
        }

        return builder.build();
    }
}
