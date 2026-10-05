package io.kestra.plugin.dropbox.models;

import java.util.Date;

import com.dropbox.core.v2.files.MediaInfo;
import com.dropbox.core.v2.files.VideoMetadata;

import io.kestra.core.models.annotations.PluginProperty;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class DropboxMediaInfo {
    @Schema(title = "Media type", description = "Either 'photo' or 'video'. Null while Dropbox is still extracting the media info.")
    @PluginProperty(group = "advanced")
    private final String type;

    @Schema(title = "Pending", description = "True when Dropbox has not finished extracting the media info yet; other fields are then null.")
    @PluginProperty(group = "advanced")
    private final boolean pending;

    @Schema(title = "Width in pixels")
    @PluginProperty(group = "advanced")
    private final Long width;

    @Schema(title = "Height in pixels")
    @PluginProperty(group = "advanced")
    private final Long height;

    @Schema(title = "GPS latitude")
    @PluginProperty(group = "advanced")
    private final Double latitude;

    @Schema(title = "GPS longitude")
    @PluginProperty(group = "advanced")
    private final Double longitude;

    @Schema(title = "Time the photo or video was taken")
    @PluginProperty(group = "advanced")
    private final Date timeTaken;

    @Schema(title = "Video duration in milliseconds", description = "Null for photos.")
    @PluginProperty(group = "advanced")
    private final Long duration;

    public static DropboxMediaInfo of(MediaInfo mediaInfo) {
        if (mediaInfo == null) {
            return null;
        }
        if (mediaInfo.isPending()) {
            return DropboxMediaInfo.builder().pending(true).build();
        }

        var metadata = mediaInfo.getMetadataValue();
        var builder = DropboxMediaInfo.builder()
            .timeTaken(metadata.getTimeTaken());

        if (metadata.getDimensions() != null) {
            builder.width(metadata.getDimensions().getWidth())
                .height(metadata.getDimensions().getHeight());
        }
        if (metadata.getLocation() != null) {
            builder.latitude(metadata.getLocation().getLatitude())
                .longitude(metadata.getLocation().getLongitude());
        }
        if (metadata instanceof VideoMetadata video) {
            builder.type("video").duration(video.getDuration());
        } else {
            builder.type("photo");
        }

        return builder.build();
    }
}
