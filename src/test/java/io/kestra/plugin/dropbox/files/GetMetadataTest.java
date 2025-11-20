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
class GetMetadataTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String filePath = "/file.txt";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        GetMetadataBuilder builderMock = mock(GetMetadataBuilder.class);
        FileMetadata metadataMock = mock(FileMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.getMetadataBuilder(filePath)).thenReturn(builderMock);
        when(builderMock.withIncludeMediaInfo(anyBoolean())).thenReturn(builderMock);
        when(builderMock.start()).thenReturn(metadataMock);

        when(metadataMock.getName()).thenReturn("file.txt");
        when(metadataMock.getPathLower()).thenReturn("/file.txt");
        when(metadataMock.getSize()).thenReturn(123L);

        // Task Execution
        GetMetadata task = new GetMetadata(
            Property.ofValue("fake-token"),
            filePath,
            Property.ofValue(false)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        GetMetadata.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getFile().getName(), is("file.txt"));
        assertThat(output.getFile().getId(), is("/file.txt"));
        assertThat(output.getFile().getSize(), is(123L));
    }

    @Test
    void run_pathNotFound_throwsException() throws Exception {
        RunContext runContext = runContextFactory.of();
        String filePath = "/not_found.txt";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        GetMetadataBuilder builderMock = mock(GetMetadataBuilder.class);

        GetMetadataError getMetadataError = GetMetadataError.path(LookupError.NOT_FOUND);
        LocalizedText userMessage = new LocalizedText("Not found.", Locale.ENGLISH.toLanguageTag());
        GetMetadataErrorException getMetadataErrorException = new GetMetadataErrorException(
            "get_metadata", "test-request-id", userMessage, getMetadataError
        );

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.getMetadataBuilder(filePath)).thenReturn(builderMock);
        when(builderMock.withIncludeMediaInfo(anyBoolean())).thenReturn(builderMock);
        when(builderMock.start()).thenThrow(getMetadataErrorException);

        // Task Execution
        GetMetadata task = new GetMetadata(
            Property.ofValue("fake-token"),
            filePath,
            Property.ofValue(false)
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