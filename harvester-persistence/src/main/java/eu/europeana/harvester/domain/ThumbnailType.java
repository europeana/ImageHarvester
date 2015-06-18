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
}
