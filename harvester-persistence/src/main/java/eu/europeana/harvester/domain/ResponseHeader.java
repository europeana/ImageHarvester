package eu.europeana.harvester.domain;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Represents an HTTP header.
 */
public class ResponseHeader implements Serializable {

    /**
     * Field name.
     */
    private final String key;

    /**
     * Field value.
     */
    private final String value;

    public ResponseHeader() {
        this.key = null;
        this.value = null;
    }

    public ResponseHeader(final String key, final String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }
}
