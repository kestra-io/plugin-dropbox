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

import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class CreateFolderTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String folderPath = "/new_folder";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        FolderMetadata metadataMock = mock(FolderMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.createFolder(folderPath, false)).thenReturn(metadataMock);

        when(metadataMock.getName()).thenReturn("new_folder");
        when(metadataMock.getPathLower()).thenReturn("/new_folder");

        // Task Execution
        CreateFolder task = new CreateFolder(
            Property.ofValue("fake-token"),
            folderPath,
            Property.ofValue(false)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        CreateFolder.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getFile().getName(), is("new_folder"));
        assertThat(output.getFile().getId(), is("/new_folder"));
    }

    @Test
    void run_pathConflict_throwsException() throws Exception {
        RunContext runContext = runContextFactory.of();
        String folderPath = "/already_exists";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);

        WriteConflictError conflictError = WriteConflictError.FILE;
        WriteError writeError = WriteError.conflict(conflictError);
        CreateFolderError createFolderError = CreateFolderError.path(writeError);

        LocalizedText userMessage = new LocalizedText("Conflict.", Locale.ENGLISH.toLanguageTag());
        CreateFolderErrorException createFolderErrorException = new CreateFolderErrorException(
            "create_folder_v2", "test-request-id", userMessage, createFolderError
        );

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.createFolder(folderPath, false)).thenThrow(createFolderErrorException);

        // Task Execution
        CreateFolder task = new CreateFolder(
            Property.ofValue("fake-token"),
            folderPath,
            Property.ofValue(false)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        // Assertion
        Exception exception = Assertions.assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("A file or folder already exists at path"));
    }
}