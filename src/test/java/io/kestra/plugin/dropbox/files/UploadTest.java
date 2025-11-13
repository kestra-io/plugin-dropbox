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
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.*;

@KestraTest
class UploadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String fileContent = "Upload test content";
        String dropboxPath = "/target/upload.txt";
        InputStream contentStream = new ByteArrayInputStream(fileContent.getBytes(StandardCharsets.UTF_8));
        URI kestraUri = runContext.storage().putFile(contentStream, "upload.txt");

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        UploadBuilder uploadBuilderMock = mock(UploadBuilder.class);
        FileMetadata metadataMock = mock(FileMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.uploadBuilder(dropboxPath)).thenReturn(uploadBuilderMock);
        when(uploadBuilderMock.withMode(any(WriteMode.class))).thenReturn(uploadBuilderMock);
        when(uploadBuilderMock.withAutorename(anyBoolean())).thenReturn(uploadBuilderMock);
        when(metadataMock.getName()).thenReturn("upload.txt");
        when(metadataMock.getSize()).thenReturn((long) fileContent.length());

        when(uploadBuilderMock.uploadAndFinish(any(InputStream.class))).thenAnswer((Answer<FileMetadata>) invocation -> {
            return metadataMock;
        });

        // Task Execution
        Upload task = new Upload(
            Property.ofValue("fake-token"),
            kestraUri,
            Property.ofValue(dropboxPath),
            Property.ofValue("OVERWRITE"),
            Property.ofValue(false)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Upload.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getMetadata(), notNullValue());
        assertThat(output.getMetadata().getName(), is("upload.txt"));

        ArgumentCaptor<WriteMode> modeCaptor = ArgumentCaptor.forClass(WriteMode.class);
        verify(uploadBuilderMock).withMode(modeCaptor.capture());
        assertThat(modeCaptor.getValue(), is(WriteMode.OVERWRITE));
    }


    @Test
    void run_uploadError_throwsException() throws Exception {
        RunContext runContext = runContextFactory.of();
        String dropboxPath = "/target/fail.txt";
        InputStream contentStream = new ByteArrayInputStream("fail".getBytes(StandardCharsets.UTF_8));
        URI kestraUri = runContext.storage().putFile(contentStream, "fail.txt");

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        UploadBuilder uploadBuilderMock = mock(UploadBuilder.class);

        UploadError uploadError = UploadError.path(
            new UploadWriteFailed(WriteError.NO_WRITE_PERMISSION, "test-session-id")
        );
        LocalizedText userMessage = new LocalizedText("Permission denied", Locale.ENGLISH.toLanguageTag());

        UploadErrorException uploadErrorException = new UploadErrorException(
            "upload",
            "test-request-id",
            userMessage,
            uploadError
        );

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.uploadBuilder(dropboxPath)).thenReturn(uploadBuilderMock);
        when(uploadBuilderMock.withMode(any(WriteMode.class))).thenReturn(uploadBuilderMock);
        when(uploadBuilderMock.withAutorename(anyBoolean())).thenReturn(uploadBuilderMock);
        when(uploadBuilderMock.uploadAndFinish(any(InputStream.class))).thenThrow(uploadErrorException);

        // Task Execution
        Upload task = new Upload(
            Property.ofValue("fake-token"),
            kestraUri, // Pass the URI object as 'from'
            Property.ofValue(dropboxPath),
            Property.ofValue("ADD"),
            Property.ofValue(false)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        // Assertion
        Exception exception = Assertions.assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("Could not upload file"));
    }
}