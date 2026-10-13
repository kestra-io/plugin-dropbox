package io.kestra.plugin.dropbox.models;

import org.junit.jupiter.api.Test;

import com.dropbox.core.v2.files.Dimensions;
import com.dropbox.core.v2.files.MediaInfo;
import com.dropbox.core.v2.files.VideoMetadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class DropboxMediaInfoTest {
    @Test
    void of_null_returnsNull() {
        assertThat(DropboxMediaInfo.of(null), nullValue());
    }

    @Test
    void of_pending_onlySetsPending() {
        var mediaInfo = DropboxMediaInfo.of(MediaInfo.PENDING);

        assertThat(mediaInfo.isPending(), is(true));
        assertThat(mediaInfo.getType(), nullValue());
        assertThat(mediaInfo.getWidth(), nullValue());
    }

    @Test
    void of_video_mapsDurationAndSkipsMissingLocation() {
        var mediaInfo = DropboxMediaInfo.of(MediaInfo.metadata(new VideoMetadata(new Dimensions(720, 1280), null, null, 42_000L)));

        assertThat(mediaInfo.getType(), is("video"));
        assertThat(mediaInfo.getWidth(), is(1280L));
        assertThat(mediaInfo.getHeight(), is(720L));
        assertThat(mediaInfo.getDuration(), is(42_000L));
        assertThat(mediaInfo.getLatitude(), nullValue());
        assertThat(mediaInfo.getTimeTaken(), nullValue());
    }
}
