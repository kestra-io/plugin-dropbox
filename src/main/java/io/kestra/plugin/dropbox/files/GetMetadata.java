package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.GetMetadataErrorException;
import com.dropbox.core.v2.files.Metadata;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.dropbox.models.DropboxFile;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;

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
        )
    }
)
@Schema(
    title = "Retrieve Dropbox item metadata",
    description = "Retrieves metadata for a Dropbox file or folder. Path must start with `/` or come from a kestra:// URI. Optional media info disabled by default."
)
public class GetMetadata extends Task implements RunnableTask<GetMetadata.Output> {

    @Schema(title = "Dropbox access token", description = "Token must allow reading the target path.")
    @NotNull
    private Property<String> accessToken;

    @Schema(
        title = "Path to inspect",
        description = "Literal Dropbox path or kestra:// URI containing the path. Must start with `/`."
    )
    @NotNull
    private Object path;

    @Schema(title = "Include media info", description = "Default false. When true, returns media info if available.")
    @Builder.Default
    private Property<Boolean> includeMediaInfo = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rPath = this.renderPath(runContext, this.path, "path");
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

    private String renderPath(RunContext runContext, Object pathSource, String fieldName) throws Exception {
        String rPath;
        String renderedFrom = runContext.render(String.valueOf(pathSource));

        if (renderedFrom == null || renderedFrom.isBlank()) {
            throw new IllegalArgumentException("'" + fieldName + "' input is required and cannot be empty.");
        }

        if (renderedFrom.startsWith("kestra://")) {
            URI fromUri = URI.create(renderedFrom);
            try (InputStream inputStream = runContext.storage().getFile(fromUri)) {
                rPath = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).trim();
            }
        } else {
            rPath = renderedFrom;
        }

        if (rPath.isBlank()) {
            throw new IllegalArgumentException("'" + fieldName + "' resolved to an empty path");
        }
        if (!rPath.startsWith("/")) {
            throw new IllegalArgumentException("'" + fieldName + "' path must start with '/'");
        }
        return rPath;
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
