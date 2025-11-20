package io.kestra.plugin.dropbox.files;

import com.dropbox.core.LocalizedText;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.*;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class DeleteTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_fromString_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String dropboxPath = "/to_delete.txt";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DeleteResult deleteResultMock = mock(DeleteResult.class);
        FileMetadata metadataMock = mock(FileMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.deleteV2(dropboxPath)).thenReturn(deleteResultMock);
        when(deleteResultMock.getMetadata()).thenReturn(metadataMock);
        when(metadataMock.getName()).thenReturn("to_delete.txt");
        when(metadataMock.getPathLower()).thenReturn("/to_delete.txt");

        // Task Execution
        Delete task = new Delete(
            Property.ofValue("fake-token"),
            dropboxPath
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Delete.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getFile().getName(), is("to_delete.txt"));
        assertThat(output.getFile().getId(), is("/to_delete.txt"));
    }

    @Test
    void run_fromURI_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String dropboxPath = "/uri/to_delete.txt";
        InputStream pathContentStream = new ByteArrayInputStream(dropboxPath.getBytes(StandardCharsets.UTF_8));
        URI pathFileUri = runContext.storage().putFile(pathContentStream, "path.txt");

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DeleteResult deleteResultMock = mock(DeleteResult.class);
        FileMetadata metadataMock = mock(FileMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.deleteV2(dropboxPath)).thenReturn(deleteResultMock);
        when(deleteResultMock.getMetadata()).thenReturn(metadataMock);
        when(metadataMock.getName()).thenReturn("to_delete.txt");

        // Task Execution
        Delete task = new Delete(
            Property.ofValue("fake-token"),
            pathFileUri
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Delete.Output output = task.run(runContext);
        assertThat(output.getFile().getName(), is("to_delete.txt"));
    }

    @Test
    void run_pathNotFound_throwsException() throws Exception {
        RunContext runContext = runContextFactory.of();
        String dropboxPath = "/not_found.txt";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);

        DeleteError deleteError = DeleteError.pathLookup(LookupError.NOT_FOUND);
        LocalizedText userMessage = new LocalizedText("File not found.", Locale.ENGLISH.toLanguageTag());
        DeleteErrorException deleteErrorException = new DeleteErrorException(
            "delete_v2", "test-request-id", userMessage, deleteError
        );

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.deleteV2(dropboxPath)).thenThrow(deleteErrorException);

        // Task Execution
        Delete task = new Delete(
            Property.ofValue("fake-token"),
            dropboxPath
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        // Assertion
        Exception exception = Assertions.assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("File or folder not found at Dropbox path"));
    }
}