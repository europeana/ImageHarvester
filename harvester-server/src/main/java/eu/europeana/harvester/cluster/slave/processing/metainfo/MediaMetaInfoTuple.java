package eu.europeana.harvester.cluster.slave.processing.metainfo;

import eu.europeana.harvester.domain.AudioMetaInfo;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.TextMetaInfo;
import eu.europeana.harvester.domain.VideoMetaInfo;

public class MediaMetaInfoTuple {

    /**
     * Stores meta information about an image type document (width, height, mimetype, fileformat, colorspace,
     * filesize, colorPalette, orientation).
     */
    private final ImageMetaInfo imageMetaInfo;

    /**
     * Stores meta information about an audio type document (samplerate, bitrate, duration, fileformat,
     * mimetype, filesize, channels, bitdepth).
     */
    private final AudioMetaInfo audioMetaInfo;

    /**
     * Stores meta information about a video type document (width, height, duration,
     * framerate, mimetype, filesize, codec, resolution).
     */
    private final VideoMetaInfo videoMetaInfo;

    /**
     * Stores meta information about a text type document (mimetype, filesize, resolution).
     */
    private final TextMetaInfo textMetaInfo;

    public MediaMetaInfoTuple(ImageMetaInfo imageMetaInfo, AudioMetaInfo audioMetaInfo, VideoMetaInfo videoMetaInfo, TextMetaInfo textMetaInfo) {
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
        this.textMetaInfo = textMetaInfo;
    }

    public ImageMetaInfo getImageMetaInfo() {
        return imageMetaInfo;
    }

    public AudioMetaInfo getAudioMetaInfo() {
        return audioMetaInfo;
    }

    public VideoMetaInfo getVideoMetaInfo() {
        return videoMetaInfo;
    }

    public TextMetaInfo getTextMetaInfo() {
        return textMetaInfo;
    }

    public boolean isValid() {
        return null != getAudioMetaInfo() ||
                null != getImageMetaInfo() ||
                null != getTextMetaInfo() ||
                null != getVideoMetaInfo();
    }

    public MediaMetaInfoTuple withImageMetaInfo(ImageMetaInfo newImageMetaInfo) {
        return new MediaMetaInfoTuple(newImageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo);
    }
}