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
import org.mockito.stubbing.Answer;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class DownloadTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_fromString_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String fileContent = "Hello from Dropbox!";
        String filePath = "/data/in/file.csv";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DownloadBuilder downloadBuilderMock = mock(DownloadBuilder.class);
        FileMetadata metadataMock = mock(FileMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.downloadBuilder(filePath)).thenReturn(downloadBuilderMock);
        when(metadataMock.getName()).thenReturn("file.csv");
        when(metadataMock.getSize()).thenReturn((long) fileContent.length());

        when(downloadBuilderMock.download(any(OutputStream.class))).thenAnswer((Answer<FileMetadata>) invocation -> {
            OutputStream providedOutputStream = invocation.getArgument(0);
            providedOutputStream.write(fileContent.getBytes(StandardCharsets.UTF_8));
            return metadataMock;
        });

        // Task Execution
        Download task = new Download(
            Property.ofValue("fake-token"),
            filePath
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Download.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getMetadata().getName(), is("file.csv"));
        assertThat(output.getMetadata().getSize(), is((long) fileContent.length()));

        // Verify content
        try (var storedInputStream = runContext.storage().getFile(output.getUri());
             var reader = new BufferedReader(new InputStreamReader(storedInputStream, StandardCharsets.UTF_8))) {
            String storedContent = reader.readLine();
            assertThat(storedContent, is(fileContent));
        }
    }

    @Test
    void run_fromURI_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String fileContent = "URI test content";
        String filePath = "/uri/path/data.txt";
        InputStream pathContentStream = new ByteArrayInputStream(filePath.getBytes(StandardCharsets.UTF_8));
        URI pathFileUri = runContext.storage().putFile(pathContentStream, "path.txt");


        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DownloadBuilder downloadBuilderMock = mock(DownloadBuilder.class);
        FileMetadata metadataMock = mock(FileMetadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.downloadBuilder(filePath)).thenReturn(downloadBuilderMock);
        when(metadataMock.getName()).thenReturn("data.txt");
        when(metadataMock.getSize()).thenReturn((long) fileContent.length());

        when(downloadBuilderMock.download(any(OutputStream.class))).thenAnswer((Answer<FileMetadata>) invocation -> {
            OutputStream providedOutputStream = invocation.getArgument(0);
            providedOutputStream.write(fileContent.getBytes(StandardCharsets.UTF_8));
            return metadataMock;
        });


        // Task Execution
        Download task = new Download(
            Property.ofValue("fake-token"),
            pathFileUri
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Download.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getMetadata().getName(), is("data.txt"));

        // Verify content
        try (var storedInputStream = runContext.storage().getFile(output.getUri());
             var reader = new BufferedReader(new InputStreamReader(storedInputStream, StandardCharsets.UTF_8))) {
            String storedContent = reader.readLine();
            assertThat(storedContent, is(fileContent));
        }
    }


    @Test
    void run_downloadError_throwsException() throws Exception {
        RunContext runContext = runContextFactory.of();
        String filePath = "/non/existent/file.zip";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DownloadBuilder downloadBuilderMock = mock(DownloadBuilder.class);

        DownloadError downloadError = DownloadError.path(LookupError.NOT_FOUND);
        LocalizedText userMessage = new LocalizedText("File not found.", Locale.ENGLISH.toLanguageTag());

        DownloadErrorException downloadErrorException = new DownloadErrorException(
            "download",
            "test-request-id",
            userMessage,
            downloadError
        );

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.downloadBuilder(filePath)).thenReturn(downloadBuilderMock);
        when(downloadBuilderMock.download(any(OutputStream.class))).thenThrow(downloadErrorException);

        // Task Execution
        Download task = new Download(
            Property.ofValue("fake-token"),
            filePath
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        // Assertion
        Exception exception = Assertions.assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), containsString("File not found at Dropbox path"));
    }
}