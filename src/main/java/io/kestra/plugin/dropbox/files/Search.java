package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.SearchErrorException;
import com.dropbox.core.v2.files.SearchMatchV2;
import com.dropbox.core.v2.files.SearchOptions;
import com.dropbox.core.v2.files.SearchV2Result;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
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
            title = "Search for files matching a query.",
            full = true,
            code = """
                id: dropbox_search
                namespace: company.team
                tasks:
                  - id: search_for_reports
                    type: io.kestra.plugin.dropbox.files.Search
                    accessToken: "{{ secrets.DROPBOX_ACCESS_TOKEN }}"
                    query: "report.csv"
                    path: "/reports"
                    fetchType: FETCH
                """
        )
    }
)
@Schema(title = "Search for files and folders in Dropbox.")
public class Search extends Task implements RunnableTask<Search.Output> {

    @Schema(title = "Dropbox access token.")
    @NotNull
    private Property<String> accessToken;

    @Schema(title = "The string to search for.")
    @NotNull
    private Property<String> query;

    @Schema(
        title = "The path to search in (optional).",
        description = "Can be a direct path as a string, or a Kestra internal storage URI (kestra://...) of a file containing the path."
    )
    private Object path;

    @Schema(title = "The maximum number of results to return.")
    private Property<Integer> maxResults;

    @Schema(title = "Restricts search to only files with the given extensions (e.g., 'jpg', 'png').")
    private Property<List<String>> fileExtensions;

    @Schema(
        title = "How to fetch the data.",
        description = "FETCH_ONE: Returns a single row.\n" +
            "FETCH: Returns all rows in memory.\n" +
            "STORE: Returns all rows in a file stored in Kestra's internal storage."
    )
    @Builder.Default
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);


    @Override
    @SuppressWarnings("unchecked")
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rQuery = runContext.render(this.query).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'query' is required"));
        String rPath = this.path == null ? null : this.renderPath(runContext, this.path, "path");
        Integer rMaxResults = this.maxResults == null ? null : runContext.render(this.maxResults).as(Integer.class).orElse(null);

        List<String> rFileExtensions = null;
        if (this.fileExtensions != null) {
            List<?> rawList = runContext.render(this.fileExtensions).as((Class<List<String>>) (Class<?>) List.class).orElse(null);
            if (rawList != null) {
                rFileExtensions = (List<String>) rawList;
            }
        }

        FetchType rFetchType = runContext.render(this.fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        DbxClientV2 client = this.createClient(runContext);

        try {
            logger.info("Searching Dropbox for query: '{}'", rQuery);

            SearchOptions.Builder optionsBuilder = SearchOptions.newBuilder();
            if (rPath != null && !rPath.isEmpty()) {
                optionsBuilder.withPath(rPath);
            }
            if (rMaxResults != null) {
                optionsBuilder.withMaxResults(rMaxResults.longValue());
            }
            if (rFileExtensions != null && !rFileExtensions.isEmpty()) {
                optionsBuilder.withFileExtensions(rFileExtensions);
            }

            SearchV2Result result = client.files().searchV2Builder(rQuery)
                .withOptions(optionsBuilder.build())
                .start();

            List<SearchMatchV2> allMatches = new ArrayList<>();
            while (true) {
                allMatches.addAll(result.getMatches());

                if (!result.getHasMore() || (rFetchType == FetchType.FETCH_ONE && !allMatches.isEmpty())) {
                    break;
                }

                result = client.files().searchContinueV2(result.getCursor());
            }

            long size = allMatches.size();
            runContext.metric(Counter.of("files.count", size));
            logger.debug("Found {} search results.", size);

            Output.OutputBuilder<?, ?> outputBuilder = Output.builder().size(size);

            List<DropboxFile> dropboxFiles = allMatches.stream()
                .map(match -> DropboxFile.of(match.getMetadata().getMetadataValue()))
                .collect(Collectors.toList());

            switch (rFetchType) {
                case FETCH_ONE:
                    if (!dropboxFiles.isEmpty()) {
                        outputBuilder.row(dropboxFiles.get(0));
                    }
                    break;
                case FETCH:
                    outputBuilder.rows(dropboxFiles);
                    break;
                case STORE:
                    File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                    try (OutputStream outputStream = new FileOutputStream(tempFile)) {
                        for (DropboxFile file : dropboxFiles) {
                            FileSerde.write(outputStream, file);
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
        } catch (SearchErrorException e) {
            logger.error("Could not perform search. Error: {}", e.errorValue, e);
            throw new Exception("Could not perform search: " + e.errorValue.toString(), e);
        }
    }

    private String renderPath(RunContext runContext, Object pathSource, String fieldName) throws Exception {
        String rPath;
        String renderedFrom = runContext.render(String.valueOf(pathSource));

        if (renderedFrom == null || renderedFrom.isBlank()) {
            return null;
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
            return null;
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
        @Schema(title = "The list of search results.", description = "Only populated if `fetchType` is `FETCH`.")
        private final List<DropboxFile> rows;

        @Schema(title = "The first search result found.", description = "Only populated if `fetchType` is `FETCH_ONE`.")
        private final DropboxFile row;

        @Schema(title = "The URI of the file in Kestra's internal storage.", description = "Only populated if `fetchType` is `STORE`.")
        private URI uri;

        @Schema(title = "The number of results found.")
        private final long size;
    }
}