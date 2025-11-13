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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class CopyTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_fromString_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String fromPath = "/source/file.txt";
        String toPath = "/dest/file.txt";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        CopyV2Builder copyBuilderMock = mock(CopyV2Builder.class);
        RelocationResult resultMock = mock(RelocationResult.class);
        FileMetadata metadataMock = mock(FileMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.copyV2Builder(fromPath, toPath)).thenReturn(copyBuilderMock);
        when(copyBuilderMock.withAutorename(anyBoolean())).thenReturn(copyBuilderMock);
        when(copyBuilderMock.start()).thenReturn(resultMock);
        when(resultMock.getMetadata()).thenReturn(metadataMock);
        when(metadataMock.getName()).thenReturn("file.txt");
        when(metadataMock.getPathLower()).thenReturn("/dest/file.txt");

        // Task Execution
        Copy task = new Copy(
            Property.ofValue("fake-token"),
            fromPath,
            toPath,
            Property.ofValue(false)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Copy.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getFile().getName(), is("file.txt"));
        assertThat(output.getFile().getId(), is("/dest/file.txt"));
    }

    @Test
    void run_pathNotFound_throwsException() throws Exception {
        RunContext runContext = runContextFactory.of();
        String fromPath = "/not_found.txt";
        String toPath = "/dest/not_found.txt";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        CopyV2Builder copyBuilderMock = mock(CopyV2Builder.class);

        RelocationError relocationError = RelocationError.fromLookup(LookupError.NOT_FOUND); // CORRECTED
        LocalizedText userMessage = new LocalizedText("File not found.", Locale.ENGLISH.toLanguageTag());
        RelocationErrorException copyErrorException = new RelocationErrorException( // CORRECTED
            "copy_v2", "test-request-id", userMessage, relocationError
        );

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.copyV2Builder(fromPath, toPath)).thenReturn(copyBuilderMock);
        when(copyBuilderMock.withAutorename(anyBoolean())).thenReturn(copyBuilderMock);
        when(copyBuilderMock.start()).thenThrow(copyErrorException);

        // Task Execution
        Copy task = new Copy(
            Property.ofValue("fake-token"),
            fromPath,
            toPath,
            Property.ofValue(false)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        // Assertion
        Exception exception = Assertions.assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Source path not found"));
    }
}