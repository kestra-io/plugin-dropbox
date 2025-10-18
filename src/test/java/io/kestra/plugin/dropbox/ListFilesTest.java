package io.kestra.plugin.dropbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DbxUserFilesRequests;
import com.dropbox.core.v2.files.DbxUserListFolderBuilder;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.ListFolderResult;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class ListFilesTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ObjectMapper objectMapper;

    @Test
    void run_fetch_fromString_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DbxUserListFolderBuilder builderMock = mock(DbxUserListFolderBuilder.class);
        FileMetadata fakeFile = mock(FileMetadata.class);
        when(fakeFile.getName()).thenReturn("test.txt");
        when(fakeFile.getPathLower()).thenReturn("id:1234");
        when(fakeFile.getSize()).thenReturn(123L);
        ListFolderResult fakeResult = new ListFolderResult(Collections.singletonList(fakeFile), "fake_cursor", false);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.listFolderBuilder(anyString())).thenReturn(builderMock);
        when(builderMock.withRecursive(anyBoolean())).thenReturn(builderMock);
        when(builderMock.start()).thenReturn(fakeResult);

        ListFiles task = new ListFiles(
            Property.ofValue("fake-token"),
            "/test-path",
            Property.ofValue(false),
            Property.ofValue(100),
            Property.ofValue(FetchType.FETCH)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        ListFiles.Output output = task.run(runContext);

        assertThat(output.getRows().size(), is(1));
        assertThat(output.getRows().getFirst().getId(), is("id:1234"));
    }

    @Test
    void run_fetch_fromURI_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        InputStream pathContent = new ByteArrayInputStream("/uri/path".getBytes(StandardCharsets.UTF_8));
        URI pathFileUri = runContext.storage().putFile(pathContent, "path.txt");

        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DbxUserListFolderBuilder builderMock = mock(DbxUserListFolderBuilder.class);
        FileMetadata fakeFile = mock(FileMetadata.class);
        when(fakeFile.getName()).thenReturn("test-uri.txt");
        ListFolderResult fakeResult = new ListFolderResult(Collections.singletonList(fakeFile), "fake_cursor", false);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.listFolderBuilder(anyString())).thenReturn(builderMock);
        when(builderMock.withRecursive(anyBoolean())).thenReturn(builderMock);
        when(builderMock.start()).thenReturn(fakeResult);

        ListFiles task = new ListFiles(
            Property.ofValue("fake-token"),
            pathFileUri,
            Property.ofValue(false),
            Property.ofValue(100),
            Property.ofValue(FetchType.FETCH)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        ListFiles.Output output = task.run(runContext);

        assertThat(output.getRows().size(), is(1));
        assertThat(output.getRows().getFirst().getName(), is("test-uri.txt"));
    }

    @Test
    void run_store_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DbxUserListFolderBuilder builderMock = mock(DbxUserListFolderBuilder.class);
        FileMetadata fakeFile = mock(FileMetadata.class);
        when(fakeFile.getName()).thenReturn("test.txt");
        when(fakeFile.getPathLower()).thenReturn("id:1234");
        when(fakeFile.getSize()).thenReturn(123L);
        ListFolderResult fakeResult = new ListFolderResult(Collections.singletonList(fakeFile), "fake_cursor", false);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.listFolderBuilder(anyString())).thenReturn(builderMock);
        when(builderMock.withRecursive(anyBoolean())).thenReturn(builderMock);
        when(builderMock.start()).thenReturn(fakeResult);

        ListFiles task = new ListFiles(
            Property.ofValue("fake-token"),
            "/test-path",
            Property.ofValue(false),
            Property.ofValue(100),
            Property.ofValue(FetchType.STORE)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        ListFiles.Output output = task.run(runContext);

        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is(1L));

        try (InputStream inputStream = runContext.storage().getFile(output.getUri())) {
            List<Map> results = FileSerde.readAll(new BufferedReader(new InputStreamReader(inputStream)), Map.class).collectList().block();
            Map<String, Object> deserializedObject = results.getFirst();

            assertThat(deserializedObject.get("name"), is("test.txt"));
            assertThat(deserializedObject.get("id"), is("id:1234"));
        }
    }

    @Test
    void run_invalidToken_throwsException() throws Exception {
        RunContext runContext = runContextFactory.of();
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        DbxUserListFolderBuilder builderMock = mock(DbxUserListFolderBuilder.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.listFolderBuilder(anyString())).thenReturn(builderMock);
        when(builderMock.withRecursive(anyBoolean())).thenReturn(builderMock);
        when(builderMock.start()).thenThrow(new InvalidAccessTokenException("test-request-id", "invalid token", null));

        ListFiles task = new ListFiles(
            Property.ofValue("fake-token"),
            "/test-path",
            Property.ofValue(false),
            Property.ofValue(100),
            Property.ofValue(FetchType.FETCH)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(exception.getMessage(), is("Invalid Dropbox Access Token. Please check your secret or token."));
    }
}