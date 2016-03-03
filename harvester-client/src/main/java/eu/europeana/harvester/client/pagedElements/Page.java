package eu.europeana.harvester.client.pagedElements;

/**
 * @author Alexandru Stefanica, alexandru.stefanica@busymachines.com
 * @since 03 Mar 2016
 */


public class Page {
    private final int offset_;
    private final int maxSize_;

    public Page(int offset, int maxSize) {
        offset_ = offset;
        maxSize_ = maxSize;
    }

    public Page(int offset) {
        this(offset, 20);
    }

    public int getOffset() {
        return offset_;
    }

    public int getMaxSize() {
        return maxSize_;
    }
}
