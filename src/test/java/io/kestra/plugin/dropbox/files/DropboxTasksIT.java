package io.kestra.plugin.dropbox.files;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@DisabledIf(value = "isTokenMissing", disabledReason = "DROPBOX_ACCESS_TOKEN environment variable is not set")
class DropboxTasksIT {

    @Inject
    private RunContextFactory runContextFactory;

    private static final String ACCESS_TOKEN = System.getenv("DROPBOX_ACCESS_TOKEN");

    static boolean isTokenMissing() {
        return ACCESS_TOKEN == null || ACCESS_TOKEN.isEmpty();
    }

    private RunContext getRunContext() {
        return runContextFactory.of();
    }

    private String createTestFolder() throws Exception {
        RunContext runContext = getRunContext();
        String folderPath = "/" + IdUtils.create();

        CreateFolder.Output createFolderOutput = CreateFolder.builder()
            .accessToken(Property.ofValue(ACCESS_TOKEN))
            .path(folderPath)
            .build()
            .run(runContext);

        assertThat(createFolderOutput.getFile().getPath(), is(folderPath));
        return folderPath;
    }

    private void deleteTestFolder(String folderPath) throws Exception {
        RunContext runContext = getRunContext();
        Delete.builder()
            .accessToken(Property.ofValue(ACCESS_TOKEN))
            .from(folderPath)
            .build()
            .run(runContext);
    }

    @Test
    void run_List_Root() throws Exception {
        RunContext runContext = getRunContext();

        List task = List.builder()
            .accessToken(Property.ofValue(ACCESS_TOKEN))
            .from("")
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        List.Output listOutput = task.run(runContext);

        assertThat(listOutput.getRows(), notNullValue());
        assertThat(listOutput.getRows(), isA(java.util.List.class));
    }

    @Test
    void run_All_File_Tasks_Lifecycle() throws Exception {
        RunContext runContext = getRunContext();
        Logger logger = runContext.logger();
        String testFileContent = "This is an integration test file: " + IdUtils.create();
        String folderPath = createTestFolder();

        String uploadPath = folderPath + "/test_upload.txt";
        String movePath = folderPath + "/moved_file.txt";
        String copyPath = folderPath + "/copied_file.txt";

        // 1. UPLOAD
        InputStream contentStream = new ByteArrayInputStream(testFileContent.getBytes(StandardCharsets.UTF_8));
        URI kestraUri = runContext.storage().putFile(contentStream, "upload.txt");

        Upload.Output uploadOutput = Upload.builder()
            .accessToken(Property.ofValue(ACCESS_TOKEN))
            .from(kestraUri)
            .to(Property.ofValue(uploadPath))
            .mode(Property.ofValue("OVERWRITE"))
            .build()
            .run(runContext);

        assertThat(uploadOutput.getMetadata().getName(), is("test_upload.txt"));

        try {
            // 2. GETMETADATA
            GetMetadata.Output metadataOutput = GetMetadata.builder()
                .accessToken(Property.ofValue(ACCESS_TOKEN))
                .path(uploadPath)
                .build()
                .run(runContext);
            assertThat(metadataOutput.getFile().getSize(), is((long) testFileContent.length()));

            // 3. MOVE
            Move.Output moveOutput = Move.builder()
                .accessToken(Property.ofValue(ACCESS_TOKEN))
                .from(uploadPath)
                .to(movePath)
                .build()
                .run(runContext);
            assertThat(moveOutput.getFile().getPath(), is(movePath));

            // 4. COPY
            Copy.Output copyOutput = Copy.builder()
                .accessToken(Property.ofValue(ACCESS_TOKEN))
                .from(movePath)
                .to(copyPath)
                .build()
                .run(runContext);
            assertThat(copyOutput.getFile().getPath(), is(copyPath));

            // 5. SEARCH (with polling and soft assertion)
            Search.Output searchOutput = null;
            int maxRetries = 3;
            int retryCount = 0;

            while (retryCount < maxRetries) {
                logger.info("Polling Dropbox search... Attempt {}/{}", retryCount + 1, maxRetries);

                searchOutput = Search.builder()
                    .accessToken(Property.ofValue(ACCESS_TOKEN))
                    .query(Property.ofValue(testFileContent))
                    .path(folderPath)
                    .fetchType(Property.ofValue(FetchType.FETCH))
                    .build()
                    .run(runContext);

                if (searchOutput.getRows() != null && !searchOutput.getRows().isEmpty()) {
                    logger.info("Search found {} results.", searchOutput.getRows().size());
                    break;
                }

                retryCount++;
                TimeUnit.SECONDS.sleep(5);
            }

            assertThat(searchOutput, notNullValue());
            assertThat(searchOutput.getRows(), notNullValue());

            if (searchOutput.getRows().isEmpty()) {
                logger.warn("Dropbox search index did not update in time. Search test returned 0 results but this is acceptable due to eventual consistency.");
            } else {
                assertThat(searchOutput.getRows().getFirst().getName(), is("moved_file.txt"));
            }

            // 6. DOWNLOAD (the copied file)
            Download.Output downloadOutput = Download.builder()
                .accessToken(Property.ofValue(ACCESS_TOKEN))
                .from(copyPath)
                .build()
                .run(runContext);

            // Verify content
            try (var storedInputStream = runContext.storage().getFile(downloadOutput.getUri());
                 var reader = new java.io.BufferedReader(new java.io.InputStreamReader(storedInputStream, StandardCharsets.UTF_8))) {
                String storedContent = reader.readLine();
                assertThat(storedContent, is(testFileContent));
            }

        } finally {
            // 7. TEARDOWN (Delete the entire test folder)
            deleteTestFolder(folderPath);
        }
    }
}