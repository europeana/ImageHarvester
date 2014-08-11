package eu.europeana.harvester.domain;

/**
 * An object used by different DAOs to avoid blocking of the database and application.
 */
public class Page {

    /**
     * The offset in the collection.
     */
    private final Integer from;

    /**
     * The number of documents.
     */
    private final Integer limit;

    public Page(final Integer from, final Integer limit) {
        this.from = from;
        this.limit = limit;
    }

    public Integer getFrom() {
        return from;
    }

    public Integer getLimit() {
        return limit;
    }
}
