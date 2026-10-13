package io.kestra.plugin.dropbox.files;

import org.slf4j.Logger;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.GetMetadataErrorException;
import com.dropbox.core.v2.files.Metadata;
import com.google.common.annotations.VisibleForTesting;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.dropbox.DropboxPaths;
import io.kestra.plugin.dropbox.models.DropboxFile;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Get the metadata for a file or folder.",
            full = true,
            code = """
                id: dropbox_get_metadata
                namespace: company.team
                tasks:
                  - id: get_file_metadata
                    type: io.kestra.plugin.dropbox.files.GetMetadata
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}"
                    path: "/kestra_uploads/my_file.txt"
                """
        ),
        @Example(
            title = "Get the dimensions and capture time of a photo.",
            full = true,
            code = """
                id: dropbox_get_media_info
                namespace: company.team
                tasks:
                  - id: get_photo_metadata
                    type: io.kestra.plugin.dropbox.files.GetMetadata
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}"
                    path: "/photos/example.jpg"
                    includeMediaInfo: true
                  - id: log_dimensions
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ outputs.get_photo_metadata.file.mediaInfo.width }}x{{ outputs.get_photo_metadata.file.mediaInfo.height }}"
                """
        )
    }
)
@Schema(
    title = "Retrieve Dropbox item metadata",
    description = "Retrieves metadata for a Dropbox file or folder. Path must start with `/` or come from a kestra:// URI."
)
public class GetMetadata extends Task implements RunnableTask<GetMetadata.Output> {

    @ToString.Exclude
    @Schema(title = "Dropbox access token", description = "Token must allow reading the target path.")
    @NotNull
    @PluginProperty(group = "main", secret = true)
    private Property<String> accessToken;

    @Schema(
        title = "Path to inspect",
        description = "Literal Dropbox path or kestra:// URI containing the path. Must start with `/`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Object path;

    @Schema(title = "Include media info", description = "Default false. When true, photo and video metadata is returned in `file.mediaInfo`.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> includeMediaInfo = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rPath = DropboxPaths.resolve(runContext, this.path, "path");
        Boolean rIncludeMediaInfo = runContext.render(this.includeMediaInfo).as(Boolean.class).orElse(false);

        DbxClientV2 client = this.createClient(runContext);

        try {
            logger.info("Getting metadata for Dropbox path: '{}'", rPath);

            Metadata metadata = client.files().getMetadataBuilder(rPath)
                .withIncludeMediaInfo(rIncludeMediaInfo)
                .start();

            logger.info("Successfully retrieved metadata for: {}", metadata.getName());

            return Output.builder()
                .file(DropboxFile.of(metadata))
                .build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (GetMetadataErrorException e) {
            if (e.errorValue.isPath() && e.errorValue.getPathValue().isNotFound()) {
                logger.error("File or folder not found at Dropbox path '{}'", rPath, e);
                throw new Exception("File or folder not found at Dropbox path: " + rPath, e);
            } else {
                logger.error("Could not get metadata for '{}'. Error: {}", rPath, e.errorValue, e);
                throw new Exception("Could not get metadata. Verify the path is valid and you have permissions. Error: " + e.errorValue.toString(), e);
            }
        }
    }

    @VisibleForTesting
    DbxClientV2 createClient(RunContext runContext) throws Exception {
        String rToken = runContext.render(this.accessToken).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("accessToken is required"));
        DbxRequestConfig config = DbxRequestConfig.newBuilder("kestra-io/plugin-dropbox").build();
        return new DbxClientV2(config, rToken);
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Metadata for the item")
        private final DropboxFile file;
    }
}
