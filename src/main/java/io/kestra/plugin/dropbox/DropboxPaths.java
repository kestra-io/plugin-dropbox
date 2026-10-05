package io.kestra.plugin.dropbox;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

public final class DropboxPaths {
    private static final String KESTRA_SCHEME_PREFIX = "kestra://";
    private static final List<String> DROPBOX_PATH_PREFIXES = List.of("/", "id:", "rev:", "ns:");

    private DropboxPaths() {
    }

    public static String resolve(RunContext runContext, Object source, String fieldName) throws IllegalVariableEvaluationException {
        return resolveOptional(runContext, source, fieldName)
            .orElseThrow(
                () -> new IllegalArgumentException(
                    "'" + fieldName + "' is required: set a Dropbox path starting with '/' or a kestra:// URI to a file containing one."
                )
            );
    }

    public static Optional<String> resolveOptional(RunContext runContext, Object source, String fieldName) throws IllegalVariableEvaluationException {
        if (source == null) {
            return Optional.empty();
        }

        var rSource = runContext.render(source.toString());
        if (rSource == null || rSource.isBlank()) {
            return Optional.empty();
        }

        var rPath = rSource.startsWith(KESTRA_SCHEME_PREFIX) ? readPath(runContext, rSource, fieldName) : rSource;
        if (rPath.isBlank()) {
            return Optional.empty();
        }
        if (DROPBOX_PATH_PREFIXES.stream().noneMatch(rPath::startsWith)) {
            throw new IllegalArgumentException(
                "'" + fieldName + "' path must start with '/' (or be a Dropbox 'id:', 'rev:' or 'ns:' reference), got: '" + rPath + "'"
            );
        }
        return Optional.of(rPath);
    }

    private static String readPath(RunContext runContext, String rUri, String fieldName) {
        try (InputStream inputStream = runContext.storage().getFile(URI.create(rUri))) {
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8).trim();
        } catch (IOException | IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to read '" + fieldName + "' Dropbox path from Kestra URI '" + rUri + "': check the URI points to an existing file.", e);
        }
    }
}
