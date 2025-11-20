package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DeleteErrorException;
import com.dropbox.core.v2.files.DeleteResult;
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
            title = "Delete a file or folder from a specific Dropbox path.",
            full = true,
            code = """
                id: dropbox_delete
                namespace: company.team
                tasks:
                  - id: delete_report
                    type: io.kestra.plugin.dropbox.files.Delete
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}
"
                    from: "/old_reports/stale_file.csv"
                """
        )
    }
)
@Schema(title = "Delete a file or folder from Dropbox.")
public class Delete extends Task implements RunnableTask<Delete.Output> {

    @Schema(title = "Dropbox access token.")
    @NotNull
    private Property<String> accessToken;

    @Schema(
        title = "The path of the file or folder to delete.",
        description = "Can be a direct path as a string (e.g., `/my/file.txt`), or a Kestra internal storage URI (kestra://...) of a file containing the path."
    )
    @NotNull
    private Object from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rPath;
        String renderedFrom = runContext.render(String.valueOf(this.from));

        if (renderedFrom == null || renderedFrom.isBlank()) {
            throw new IllegalArgumentException("'from' input is required and cannot be empty.");
        }

        if (renderedFrom.startsWith("kestra://")) {
            URI fromUri = URI.create(renderedFrom);
            try (InputStream inputStream = runContext.storage().getFile(fromUri)) {
                rPath = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).trim();
                logger.debug("Read Dropbox path '{}' from Kestra URI '{}'", rPath, fromUri);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to read Dropbox path from Kestra URI: " + renderedFrom, e);
            }
        } else {
            rPath = renderedFrom;
        }

        if (rPath.isBlank()) {
            throw new IllegalArgumentException("'from' resolved to an empty path");
        }

        if (!rPath.startsWith("/")) {
            throw new IllegalArgumentException("'from' path must start with '/'");
        }

        DbxClientV2 client = this.createClient(runContext);

        try {
            logger.info("Deleting item from Dropbox path: '{}'", rPath);

            DeleteResult result = client.files().deleteV2(rPath);

            logger.info("Successfully deleted item: {}", result.getMetadata().getName());

            return Output.builder()
                .file(DropboxFile.of(result.getMetadata()))
                .build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (DeleteErrorException e) {
            if (e.errorValue.isPathLookup() && e.errorValue.getPathLookupValue().isNotFound()) {
                logger.error("File or folder not found at Dropbox path '{}'", rPath, e);
                throw new Exception("File or folder not found at Dropbox path: " + rPath, e);
            } else {
                logger.error("Could not delete item from Dropbox '{}'. Error: {}", rPath, e.errorValue, e);
                throw new Exception("Could not delete item. Verify the path is valid and you have permissions. Error: " + e.errorValue.toString(), e);
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
        @Schema(title = "The metadata of the deleted file or folder.")
        private final DropboxFile file;
    }
}