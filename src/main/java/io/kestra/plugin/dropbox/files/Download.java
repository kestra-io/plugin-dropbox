package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DownloadErrorException;
import com.dropbox.core.v2.files.FileMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
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
            title = "Download a file from a specific Dropbox path.",
            full = true,
            code = """
                id: dropbox_download_file
                namespace: company.team
                tasks:
                  - id: download_report
                    type: io.kestra.plugin.dropbox.files.Download
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}"
                    from: "/shared_reports/quarterly_report.csv"
                """
        ),
        @Example(
            title = "Download a file using a path from a previous task's output.",
            full = true,
            code = """
                id: dropbox_download_dynamic_path
                namespace: company.team
                tasks:
                  - id: generate_path_file
                    type: io.kestra.plugin.core.storage.Write
                    content: "/dynamic/data/{{ flow.startDate }}.json"
                    # Kestra automatically generates the output URI
                  - id: download_dynamic_file
                    type: io.kestra.plugin.dropbox.files.Download
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}"
                    from: "{{ outputs.generate_path_file.uri }}" # Reference the generated URI
                """
        )
    }
)
@Schema(title = "Download a file from Dropbox.")
public class Download extends Task implements RunnableTask<Download.Output> {

    @Schema(title = "Dropbox access token.")
    @NotNull
    private Property<String> accessToken;

    @Schema(
        title = "The path of the file to download.",
        description = "Can be a direct path as a string, or a Kestra internal storage URI (kestra://...) of a file containing the path."
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

        DbxClientV2 client = this.createClient(runContext);

        File tempFile = runContext.workingDir().createTempFile().toFile();

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            logger.info("Downloading file from Dropbox path: '{}'", rPath);

            FileMetadata metadata = client.files().downloadBuilder(rPath)
                .download(outputStream);

            URI kestraUri = runContext.storage().putFile(tempFile);

            logger.info("File '{}' ({} bytes) downloaded to Kestra storage: {}", metadata.getName(), metadata.getSize(), kestraUri);

            return Output.builder()
                .uri(kestraUri)
                .metadata(metadata)
                .build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (DownloadErrorException e) {
            if (e.errorValue.isPath() && e.errorValue.getPathValue().isNotFound()) {
                logger.error("File not found at Dropbox path '{}'", rPath, e);
                throw new Exception("File not found at Dropbox path: " + rPath, e);
            } else {
                logger.error("Could not download file from Dropbox '{}'. Error: {}", rPath, e.errorValue, e);
                throw new Exception("Could not download file. Verify the path exists and you have permissions. Error: " + e.errorValue.toString(), e);
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
        @Schema(title = "The URI of the downloaded file in Kestra's internal storage.")
        private final URI uri;

        @Schema(title = "The metadata of the downloaded file from Dropbox.")
        private final FileMetadata metadata;
    }
}