package eu.europeana.harvester.domain;

public enum ThumbnailType {
 //   SMALL(180,180),
    MEDIUM(200,200),
    LARGE(400,400);

    private int width;
    private int height;

    ThumbnailType(int width, int height) {
        this.width = width;
        this.height = height;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public static ThumbnailType valueOf(int height) {
        if (MEDIUM.getHeight() == height) {
            return  MEDIUM;
        }
        else if (LARGE.getHeight() == height) {
            return LARGE;
        }

        throw new IllegalArgumentException(height + " not a valid value for size");
    }
}
