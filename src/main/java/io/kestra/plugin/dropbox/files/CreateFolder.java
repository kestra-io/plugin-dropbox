package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.CreateFolderErrorException;
import com.dropbox.core.v2.files.FolderMetadata;
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
            title = "Create a new folder in Dropbox.",
            full = true,
            code = """
                id: dropbox_create_folder
                namespace: company.team
                tasks:
                  - id: create_folder
                    type: io.kestra.plugin.dropbox.files.CreateFolder
                    accessToken: "{{ secret('DROPBOX_ACCESS_TOKEN') }}
"
                    path: "/new_project_folder/sub_folder"
                    autorename: false
                """
        )
    }
)
@Schema(title = "Create a new folder in Dropbox.")
public class CreateFolder extends Task implements RunnableTask<CreateFolder.Output> {

    @Schema(title = "Dropbox access token.")
    @NotNull
    private Property<String> accessToken;

    @Schema(
        title = "The path of the folder to create.",
        description = "Can be a direct path as a string, or a Kestra internal storage URI (kestra://...) of a file containing the path."
    )
    @NotNull
    private Object path;

    @Schema(
        title = "If there's a conflict, have the Dropbox server try to autorename the file.",
        description = "For example, appending (1) or (2).")
    @Builder.Default
    private Property<Boolean> autorename = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rPath = this.renderPath(runContext, this.path, "path");
        Boolean rAutorename = runContext.render(this.autorename).as(Boolean.class).orElse(false);

        DbxClientV2 client = this.createClient(runContext);

        try {
            logger.info("Creating folder at Dropbox path: '{}'", rPath);

            FolderMetadata metadata = client.files().createFolder(rPath, rAutorename);

            logger.info("Successfully created folder: {}", metadata.getName());

            return Output.builder()
                .file(DropboxFile.of(metadata))
                .build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (CreateFolderErrorException e) {
            String message;
            if (e.errorValue.isPath() && e.errorValue.getPathValue().isConflict()) {
                message = "A file or folder already exists at path: " + rPath;
            } else {
                message = e.errorValue.toString();
            }
            logger.error("Could not create folder. Error: {}", message, e);
            throw new Exception("Could not create folder: " + message, e);
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
        @Schema(title = "The metadata of the created folder.")
        private final DropboxFile file;
    }
}