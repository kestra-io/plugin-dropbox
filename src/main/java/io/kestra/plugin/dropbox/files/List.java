package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.ListFolderErrorException;
import com.dropbox.core.v2.files.ListFolderResult;
import com.dropbox.core.v2.files.Metadata;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.dropbox.models.DropboxFile;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

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
                    type: io.kestra.plugin.dropbox.files.List
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}
"
                    from: "/My Kestra Files/Inputs"
                    recursive: true
                    fetchType: FETCH
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
public class List extends Task implements RunnableTask<List.Output> {

    @Schema(title = "Dropbox access token.")
    @NotNull
    private Property<String> accessToken;

    @Schema(
        title = "The path to the directory to list.",
        description = "Can be a direct path as a string, or a Kestra internal storage URI of a file containing the path."
    )
    private Object from;

    @Schema(title = "Whether to list files recursively in all sub-folders.")
    @Builder.Default
    private Property<Boolean> recursive = Property.ofValue(false);

    @Schema(title = "The maximum number of files to return.")
    @Builder.Default
    private Property<Integer> limit = Property.ofValue(2000);

    @Schema(
        title = "How to fetch the data.",
        description = "FETCH_ONE: Returns a single row.\n" +
            "FETCH: Returns all rows in memory.\n" +
            "STORE: Returns all rows in a file stored in Kestra's internal storage."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rPath;
        if (this.from instanceof String) {
            rPath = runContext.render((String) this.from);
        } else if (this.from instanceof URI) {
            try (InputStream inputStream = runContext.storage().getFile((URI) this.from)) {
                rPath = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
        } else if (this.from == null) {
            rPath = "";
        } else {
            throw new IllegalArgumentException("Invalid 'from' type: " + this.from.getClass());
        }

        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        Boolean rRecursive = runContext.render(this.recursive).as(Boolean.class).orElse(false);
        Integer rLimit = runContext.render(this.limit).as(Integer.class).orElse(null);

        DbxClientV2 client = this.createClient(runContext);

        try {
            logger.info("Listing files in Dropbox path: '{}'", rPath.isEmpty() ? "/" : rPath);

            var listFolderBuilder = client.files().listFolderBuilder(rPath).withRecursive(rRecursive);

            if (rLimit != null) {
                listFolderBuilder.withLimit(rLimit.longValue());
            }

            ListFolderResult result = listFolderBuilder.start();

            java.util.List<Metadata> allEntries = new ArrayList<>();
            while (true) {
                allEntries.addAll(result.getEntries());

                if (!result.getHasMore() || (rFetchType == FetchType.FETCH_ONE && !allEntries.isEmpty())) {
                    break;
                }

                result = client.files().listFolderContinue(result.getCursor());
            }

            long size = allEntries.size();
            runContext.metric(Counter.of("files.count", size));
            logger.debug("Found {} entries.", size);

            Output.OutputBuilder<?, ?> outputBuilder = Output.builder().size(size);

            switch (rFetchType) {
                case FETCH_ONE:
                    if (!allEntries.isEmpty()) {
                        outputBuilder.row(DropboxFile.of(allEntries.get(0)));
                    }
                    break;
                case FETCH:
                    java.util.List<DropboxFile> dropboxFiles = allEntries.stream().map(DropboxFile::of).toList();
                    outputBuilder.rows(dropboxFiles);
                    break;
                case STORE:
                    File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                    try (OutputStream outputStream = new FileOutputStream(tempFile)) {
                        for (Metadata entry : allEntries) {
                            FileSerde.write(outputStream, DropboxFile.of(entry));
                        }
                    }
                    outputBuilder.uri(runContext.storage().putFile(tempFile));
                    break;
                default:
                    throw new IllegalStateException("Unexpected value: " + rFetchType);
            }

            return outputBuilder.build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (ListFolderErrorException e) {
            logger.error("Could not list Dropbox folder '{}'. Error: {}", rPath, e.errorValue);
            throw new Exception("Could not list Dropbox folder. Verify the path exists and you have permissions. Error: " + e.errorValue.toString(), e);
        }
    }

    @VisibleForTesting
    DbxClientV2 createClient(RunContext runContext) throws Exception {
        String renderedToken = runContext.render(this.accessToken).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("accessToken is required"));
        DbxRequestConfig config = DbxRequestConfig.newBuilder("kestra-io/plugin-dropbox").build();
        return new DbxClientV2(config, renderedToken);
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The list of files and folders.", description = "Only populated if `fetchType` is `FETCH`.")
        private final java.util.List<DropboxFile> rows;

        @Schema(title = "The first file or folder found.", description = "Only populated if `fetchType` is `FETCH_ONE`.")
        private final DropboxFile row;

        @Schema(title = "The URI of the file in Kestra's internal storage.", description = "Only populated if `fetchType` is `STORE`.")
        private URI uri;

        @Schema(title = "The number of files and folders found.")
        private final long size;
    }
}