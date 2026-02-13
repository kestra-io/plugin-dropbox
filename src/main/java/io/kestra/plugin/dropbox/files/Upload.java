package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.UploadErrorException;
import com.dropbox.core.v2.files.WriteMode;
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
import java.io.InputStream;
import java.io.IOException;
import java.net.URI;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Upload a file from Kestra's internal storage to Dropbox.",
            full = true,
            code = """
                id: dropbox_upload_file
                namespace: company.team
                tasks:
                  - id: create_file_to_upload
                    type: io.kestra.plugin.core.storage.Write
                    content: "This is the content of the file to be uploaded."
                  - id: upload_to_dropbox
                    type: io.kestra.plugin.dropbox.files.Upload
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}"
                    from: "{{ outputs.create_file_to_upload.uri }}" # URI from previous task
                    to: "/kestra_uploads/my_file.txt"
                    mode: "OVERWRITE"
                    autorename: false
                """
        )
    }
)
@Schema(
    title = "Upload file to Dropbox",
    description = "Uploads a file from Kestra internal storage to a Dropbox path. Destination must start with `/`. Honors `mode` (default ADD) and optional server-side `autorename` when conflicts occur."
)
public class Upload extends Task implements RunnableTask<Upload.Output> {

    @Schema(title = "Dropbox access token", description = "Token must allow writing to the destination path.")
    @NotNull
    private Property<String> accessToken;

    @Schema(
        title = "Source file URI",
        description = "Kestra storage URI string (e.g., `{{ outputs.prev_task.uri }}`) or URI object."
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Destination Dropbox path",
        description = "Must start with `/`, e.g., `/my_folder/file.txt`."
    )
    @NotNull
    private Property<String> to;

    @Schema(
        title = "Write mode on conflict",
        description = "`ADD` (default): keep existing file; new upload may be suffixed.\n`OVERWRITE`: replace existing file."
    )
    @Builder.Default
    private Property<String> mode = Property.ofValue("ADD");

    @Schema(
        title = "Auto-rename on conflict",
        description = "Default false. When true with `ADD`, Dropbox appends suffixes like (1) if the path exists."
    )
    @Builder.Default
    private Property<Boolean> autorename = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        URI rFromUri;
        String renderedFrom = runContext.render(String.valueOf(this.from));

        if (renderedFrom == null || renderedFrom.isBlank()) {
            throw new IllegalArgumentException("'from' input is required and cannot be empty.");
        }

        if (renderedFrom.startsWith("kestra://")) {
            rFromUri = URI.create(renderedFrom);
        } else if (this.from instanceof URI) {
            rFromUri = (URI) this.from;
        } else {
            throw new IllegalArgumentException("Invalid 'from' type: must be a Kestra internal storage URI (kestra://...). Got: " + this.from.getClass());
        }

        String rToPath = runContext.render(this.to).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("'to' path is required"));

        String rModeString = runContext.render(this.mode).as(String.class).orElse("ADD").toUpperCase();
        WriteMode rModeSdk;
        switch (rModeString) {
            case "OVERWRITE":
                rModeSdk = WriteMode.OVERWRITE;
                break;
            case "ADD":
                rModeSdk = WriteMode.ADD;
                break;
            default:
                throw new IllegalArgumentException("Invalid 'mode': " + rModeString + ". Must be 'ADD' or 'OVERWRITE'.");
        }

        Boolean rAutorename = runContext.render(this.autorename).as(Boolean.class).orElse(false);

        if (!rToPath.startsWith("/")) {
            throw new IllegalArgumentException("'to' path must start with '/'");
        }

        DbxClientV2 client = this.createClient(runContext);
        File tempFile = runContext.workingDir().createTempFile().toFile();

        try (InputStream inputStream = runContext.storage().getFile(rFromUri)) {
            logger.info("Uploading file from Kestra storage '{}' to Dropbox path '{}'", rFromUri, rToPath);

            FileMetadata metadata = client.files().uploadBuilder(rToPath)
                .withMode(rModeSdk)
                .withAutorename(rAutorename)
                .uploadAndFinish(inputStream);

            logger.info("File successfully uploaded to Dropbox: {}", metadata.getName());

            return Output.builder()
                .metadata(metadata)
                .build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (UploadErrorException e) {
            logger.error("Could not upload file to Dropbox '{}'. Error: {}", rToPath, e.errorValue, e);
            throw new Exception("Could not upload file. Verify the path is valid and you have permissions. Error: " + e.errorValue.toString(), e);
        } catch (IOException e) {
            logger.error("Could not read file from Kestra storage URI '{}'", rFromUri, e);
            throw new Exception("Failed to read file from Kestra storage: " + rFromUri, e);
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
        @Schema(title = "Uploaded file metadata")
        private final FileMetadata metadata;
    }
}
