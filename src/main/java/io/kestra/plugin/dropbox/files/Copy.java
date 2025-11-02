package io.kestra.plugin.dropbox.files;

import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.RateLimitException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.RelocationErrorException;
import com.dropbox.core.v2.files.RelocationResult;
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
            title = "Copy a file from one location to another.",
            full = true,
            code = """
                id: dropbox_copy
                namespace: company.team
                tasks:
                  - id: copy_file
                    type: io.kestra.plugin.dropbox.files.Copy
                    accessToken: "{{ secrets.DROPBOX_ACCESS_TOKEN }}"
                    from: "/source/template.csv"
                    to: "/reports/new_report.csv"
                    autorename: false
                """
        )
    }
)
@Schema(title = "Copy a file or folder to a different location in Dropbox.")
public class Copy extends Task implements RunnableTask<Copy.Output> {

    @Schema(title = "Dropbox access token.")
    @NotNull
    private Property<String> accessToken;

    @Schema(
        title = "The path of the file or folder to be copied.",
        description = "Can be a direct path as a string, or a Kestra internal storage URI (kestra://...) of a file containing the path."
    )
    @NotNull
    private Object from;

    @Schema(
        title = "The destination path where the file or folder should be copied.",
        description = "Can be a direct path as a string, or a Kestra internal storage URI (kestra://...) of a file containing the path."
    )
    @NotNull
    private Object to;

    @Schema(
        title = "If there's a conflict, have the Dropbox server try to autorename the file.",
        description = "For example, appending (1) or (2).")
    @Builder.Default
    private Property<Boolean> autorename = Property.ofValue(false);

    @Override
    public Output run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        String rFromPath = this.renderPath(runContext, this.from, "from");
        String rToPath = this.renderPath(runContext, this.to, "to");

        Boolean rAutorename = runContext.render(this.autorename).as(Boolean.class).orElse(false);

        DbxClientV2 client = this.createClient(runContext);

        try {
            logger.info("Copying Dropbox item from '{}' to '{}'", rFromPath, rToPath);

            RelocationResult result = client.files().copyV2Builder(rFromPath, rToPath)
                .withAutorename(rAutorename)
                .start();

            logger.info("Successfully copied item: {}", result.getMetadata().getName());

            return Output.builder()
                .file(DropboxFile.of(result.getMetadata()))
                .build();

        } catch (InvalidAccessTokenException e) {
            logger.error("Invalid Dropbox access token", e);
            throw new Exception("Invalid Dropbox Access Token. Please check your secret or token.", e);
        } catch (RateLimitException e) {
            logger.error("Dropbox API rate limit exceeded", e);
            throw new Exception("Dropbox API rate limit exceeded. Please wait before trying again.", e);
        } catch (RelocationErrorException e) { // CORRECTED: Use RelocationErrorException
            String message;
            if (e.errorValue.isFromLookup() && e.errorValue.getFromLookupValue().isNotFound()) {
                message = "Source path not found: " + rFromPath;
            } else if (e.errorValue.isTo() && e.errorValue.getToValue().isConflict()) {
                message = "A file or folder already exists at the destination path: " + rToPath;
            } else {
                message = e.errorValue.toString();
            }
            logger.error("Could not copy item. Error: {}", message, e);
            throw new Exception("Could not copy item: " + message, e);
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
        @Schema(title = "The metadata of the newly copied file or folder.")
        private final DropboxFile file;
    }
}