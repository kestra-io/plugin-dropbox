package io.kestra.plugin.dropbox.files;

import org.slf4j.Logger;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DeleteErrorException;
import com.dropbox.core.v2.files.DeleteResult;
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
            title = "Delete a file or folder from a specific Dropbox path.",
            full = true,
            code = """
                id: dropbox_delete
                namespace: company.team
                tasks:
                  - id: delete_report
                    type: io.kestra.plugin.dropbox.files.Delete
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}"
                    from: "/old_reports/stale_file.csv"
                """
        )
    }
)
@Schema(
    title = "Delete Dropbox file or folder",
    description = "Removes a Dropbox item at the resolved path. Path must start with `/`; can be read from a kestra:// URI. Fails if not found or token lacks delete permission."
)
public class Delete extends Task implements RunnableTask<Delete.Output> {

    @ToString.Exclude
    @Schema(title = "Dropbox access token", description = "Token must allow deleting the target path.")
    @NotNull
    @PluginProperty(group = "main", secret = true)
    private Property<String> accessToken;

    @Schema(
        title = "Path to delete",
        description = "Literal Dropbox path or kestra:// URI pointing to the path content. Resolved path must start with `/`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Object from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rPath = DropboxPaths.resolve(runContext, this.from, "from");

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
        @Schema(title = "Deleted item metadata")
        private final DropboxFile file;
    }
}
