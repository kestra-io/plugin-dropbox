package io.kestra.plugin.dropbox.files;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.*;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@KestraTest
class SearchTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ObjectMapper objectMapper;

    @Test
    void run_fetch_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String query = "report.csv";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        SearchV2Builder builderMock = mock(SearchV2Builder.class);
        SearchV2Result resultMock = mock(SearchV2Result.class);
        SearchMatchV2 matchMock = mock(SearchMatchV2.class);

        MetadataV2 metadataV2Mock = mock(MetadataV2.class);
        Metadata metadataMock = mock(Metadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.searchV2Builder(query)).thenReturn(builderMock);
        when(builderMock.withOptions(any(SearchOptions.class))).thenReturn(builderMock);
        when(builderMock.start()).thenReturn(resultMock);

        when(resultMock.getMatches()).thenReturn(Collections.singletonList(matchMock));
        when(resultMock.getHasMore()).thenReturn(false);

        when(matchMock.getMetadata()).thenReturn(metadataV2Mock);
        when(metadataV2Mock.getMetadataValue()).thenReturn(metadataMock);

        when(metadataMock.getName()).thenReturn("report.csv");
        when(metadataMock.getPathLower()).thenReturn("/reports/report.csv");

        // Task Execution
        Search task = new Search(
            Property.ofValue("fake-token"),
            Property.ofValue(query),
            "/reports",
            Property.ofValue(100),
            Property.ofValue(List.of("csv")),
            Property.ofValue(FetchType.FETCH)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Search.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getRows().size(), is(1));
        assertThat(output.getRows().get(0).getName(), is("report.csv"));
        assertThat(output.getSize(), is(1L));
    }

    @Test
    void run_store_success() throws Exception {
        RunContext runContext = runContextFactory.of();
        String query = "report.csv";

        // Mocking
        DbxClientV2 clientMock = mock(DbxClientV2.class);
        DbxUserFilesRequests filesRequestsMock = mock(DbxUserFilesRequests.class);
        SearchV2Builder builderMock = mock(SearchV2Builder.class);
        SearchV2Result resultMock = mock(SearchV2Result.class);
        SearchMatchV2 matchMock = mock(SearchMatchV2.class);

        MetadataV2 metadataV2Mock = mock(MetadataV2.class);
        Metadata metadataMock = mock(Metadata.class);

        when(clientMock.files()).thenReturn(filesRequestsMock);
        when(filesRequestsMock.searchV2Builder(query)).thenReturn(builderMock);
        when(builderMock.withOptions(any(SearchOptions.class))).thenReturn(builderMock);
        when(builderMock.start()).thenReturn(resultMock);
        when(resultMock.getMatches()).thenReturn(Collections.singletonList(matchMock));
        when(resultMock.getHasMore()).thenReturn(false);

        when(matchMock.getMetadata()).thenReturn(metadataV2Mock);
        when(metadataV2Mock.getMetadataValue()).thenReturn(metadataMock);
        when(metadataMock.getName()).thenReturn("report.csv");
        when(metadataMock.getPathLower()).thenReturn("/reports/report.csv");

        // Task Execution
        Search task = new Search(
            Property.ofValue("fake-token"),
            Property.ofValue(query),
            "/reports",
            null,
            null,
            Property.ofValue(FetchType.STORE)
        ) {
            @Override
            DbxClientV2 createClient(RunContext runContext) {
                return clientMock;
            }
        };

        Search.Output output = task.run(runContext);

        // Assertions
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is(1L));

        // Verify content
        try (InputStream inputStream = runContext.storage().getFile(output.getUri())) {
            List<Map> results = FileSerde.readAll(new BufferedReader(new InputStreamReader(inputStream)), Map.class).collectList().block();
            Map<String, Object> deserializedObject = results.get(0);

            assertThat(deserializedObject.get("name"), is("report.csv"));
            assertThat(deserializedObject.get("id"), is("/reports/report.csv"));
        }
    }
}