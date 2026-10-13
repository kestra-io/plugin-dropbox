package io.kestra.plugin.dropbox;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@KestraTest
class DropboxPathsTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void resolve_literalPath() throws Exception {
        var runContext = runContextFactory.of();

        assertThat(DropboxPaths.resolve(runContext, "/data/file.csv", "from"), is("/data/file.csv"));
    }

    @Test
    void resolve_renderedPath() throws Exception {
        var runContext = runContextFactory.of(Map.of("folder", "data"));

        assertThat(DropboxPaths.resolve(runContext, "/{{ folder }}/file.csv", "from"), is("/data/file.csv"));
    }

    @Test
    void resolve_kestraUriString_readsPathFromStorage() throws Exception {
        var runContext = runContextFactory.of();
        var uri = storePath(runContext, "/from/storage.csv\n");

        assertThat(DropboxPaths.resolve(runContext, uri.toString(), "from"), is("/from/storage.csv"));
    }

    @Test
    void resolve_kestraUriObject_readsPathFromStorage() throws Exception {
        var runContext = runContextFactory.of();
        var uri = storePath(runContext, "/from/storage.csv");

        assertThat(DropboxPaths.resolve(runContext, uri, "from"), is("/from/storage.csv"));
    }

    @Test
    void resolve_dropboxReferences_passThrough() throws Exception {
        var runContext = runContextFactory.of();

        for (var reference : List.of("id:a4ayc_80_OEAAAAAAAAAXw", "rev:a1c10ce0dd78", "ns:12345/team")) {
            assertThat(DropboxPaths.resolve(runContext, reference, "from"), is(reference));
        }
    }

    @Test
    void resolve_missingLeadingSlash_throws() {
        var runContext = runContextFactory.of();

        var exception = Assertions.assertThrows(IllegalArgumentException.class, () -> DropboxPaths.resolve(runContext, "data/file.csv", "from"));
        assertThat(exception.getMessage(), containsString("'from' path must start with '/'"));
        assertThat(exception.getMessage(), containsString("got: 'data/file.csv'"));
    }

    @Test
    void resolve_kestraUriWithoutLeadingSlash_throws() throws Exception {
        var runContext = runContextFactory.of();
        var uri = storePath(runContext, "data/file.csv");

        var exception = Assertions.assertThrows(IllegalArgumentException.class, () -> DropboxPaths.resolve(runContext, uri.toString(), "path"));
        assertThat(exception.getMessage(), containsString("'path' path must start with '/'"));
    }

    @Test
    void resolve_blank_throws() {
        var runContext = runContextFactory.of();

        var exception = Assertions.assertThrows(IllegalArgumentException.class, () -> DropboxPaths.resolve(runContext, " ", "to"));
        assertThat(exception.getMessage(), containsString("'to' is required"));
    }

    @Test
    void resolve_null_throws() {
        var runContext = runContextFactory.of();

        Assertions.assertThrows(IllegalArgumentException.class, () -> DropboxPaths.resolve(runContext, null, "to"));
    }

    @Test
    void resolve_unreadableKestraUri_throws() {
        var runContext = runContextFactory.of();

        var exception = Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> DropboxPaths.resolve(runContext, "kestra:///missing/path.txt", "from")
        );
        assertThat(exception.getMessage(), containsString("Failed to read 'from' Dropbox path from Kestra URI"));
    }

    @Test
    void resolve_oversizedKestraUriFile_throws() throws Exception {
        var runContext = runContextFactory.of();
        var uri = storePath(runContext, "/" + "a".repeat(4096));

        var exception = Assertions.assertThrows(IllegalArgumentException.class, () -> DropboxPaths.resolve(runContext, uri.toString(), "from"));
        assertThat(exception.getMessage(), containsString("is larger than 4096 bytes"));
    }

    @Test
    void resolve_kestraUriFileAtSizeLimit_isAccepted() throws Exception {
        var runContext = runContextFactory.of();
        var path = "/" + "a".repeat(4095);

        assertThat(DropboxPaths.resolve(runContext, storePath(runContext, path).toString(), "from"), is(path));
    }

    @Test
    void resolveOptional_nullOrBlank_isEmpty() throws Exception {
        var runContext = runContextFactory.of();

        assertThat(DropboxPaths.resolveOptional(runContext, null, "path"), is(Optional.empty()));
        assertThat(DropboxPaths.resolveOptional(runContext, "", "path"), is(Optional.empty()));
        assertThat(DropboxPaths.resolveOptional(runContext, storePath(runContext, " "), "path"), is(Optional.empty()));
    }

    private static URI storePath(RunContext runContext, String path) throws Exception {
        return runContext.storage().putFile(new ByteArrayInputStream(path.getBytes(StandardCharsets.UTF_8)), "path.txt");
    }
}
