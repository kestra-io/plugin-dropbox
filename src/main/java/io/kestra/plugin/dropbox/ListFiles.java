package io.kestra.plugin.dropbox;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.files.ListFolderErrorException;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.dropbox.models.DropboxFile;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "List all files and folders in a specific Dropbox directory.",
            full = true,
            code = """
                id: dropbox_list_files
                namespace: company.team
                tasks:
                  - id: list_files_from_dropbox
                    type: io.kestra.plugin.dropbox.ListFiles
                    accessToken: "{{ secrets.DROPBOX_ACCESS_TOKEN }}"
                    path: "/My Kestra Files/Inputs"
                    recursive: true
                """
        )
    },
    metrics = {
        @Metric(
            name = "files.count",
            type = Counter.TYPE,
            description = "The number of files and folders found."
        )
    }
)
@Schema(title = "List files and folders in a Dropbox directory.")
public class ListFiles implements RunnableTask<ListFiles.Output> {

    @Schema(title = "Dropbox access token.")
    @PluginProperty(dynamic = true)
    private String accessToken;

    @Schema(title = "The path to the directory to list.", description = "If not set, the root folder is listed.")
    @PluginProperty(dynamic = true)
    private String path;

    @Schema(title = "Whether to list files recursively in all sub-folders.")
    @PluginProperty
    @Builder.Default
    private boolean recursive = false;

    @Schema(
        title = "The maximum number of files to return.",
        description = "This is a soft limit; the server might return fewer or slightly more entries."
    )
    @PluginProperty
    private Integer limit;

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();
        String renderedPath = this.path == null ? "" : runContext.render(this.path);

        DbxClientV2 client = this.createClient(runContext);

        try {
            logger.info("Listing files in Dropbox path: '{}'", renderedPath.isEmpty() ? "/" : renderedPath);

            List<Metadata> allEntries = new ArrayList<>();
            var listFolderBuilder = client.files().listFolderBuilder(renderedPath)
                .withRecursive(this.recursive);

            if (this.limit != null) {
                listFolderBuilder.withLimit(this.limit.longValue());
            }

            ListFolderResult result = listFolderBuilder.start();

            while (true) {
                allEntries.addAll(result.getEntries());

                if (!result.getHasMore()) {
                    break;
                }

                result = client.files().listFolderContinue(result.getCursor());
            }

            List<DropboxFile> dropboxFiles = allEntries.stream()
                .map(DropboxFile::of)
                .collect(Collectors.toList());

            runContext.metric(Counter.of("files.count", dropboxFiles.size()));
            logger.debug("Found {} entries.", dropboxFiles.size());

            return Output.builder().files(dropboxFiles).build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (ListFolderErrorException e) {
            logger.error("Could not list Dropbox folder '{}'. Error: {}", renderedPath, e.errorValue);
            throw new Exception("Could not list Dropbox folder. Verify the path exists and you have permissions. Error: " + e.errorValue.toString(), e);
        }
    }

    @VisibleForTesting
    DbxClientV2 createClient(RunContext runContext) throws Exception {
        DbxRequestConfig config = DbxRequestConfig.newBuilder("kestra-io/plugin-dropbox").build();
        return new DbxClientV2(config, runContext.render(this.accessToken));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The list of files and folders found.")
        private final List<DropboxFile> files;
    }
}