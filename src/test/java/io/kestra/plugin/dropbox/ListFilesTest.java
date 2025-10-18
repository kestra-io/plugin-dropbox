package io.kestra.plugin.dropbox;

import com.dropbox.core.InvalidAccessTokenException;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.DbxUserFilesRequests;
import com.dropbox.core.v2.files.FileMetadata;
import com.dropbox.core.v2.files.DbxUserListFolderBuilder;
import com.dropbox.core.v2.files.ListFolderResult;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class ListFilesTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run_success() throws Exception {
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
            "fake-token",
            "/test-path",
            false,
            100
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        ListFiles.Output output = task.run(runContext);

        assertThat(output.getFiles().size(), is(1));
        assertThat(output.getFiles().get(0).getId(), is("id:1234"));
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
            "fake-token",
            "/test-path",
            false,
            100
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Exception exception = Assertions.assertThrows(Exception.class, () -> {
            task.run(runContext);
        });
        assertThat(exception.getMessage(), is("Invalid Dropbox Access Token. Please check your secret or token."));
    }
}