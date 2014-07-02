package eu.europeana.harvester.httpclient.response;

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
    private final ArrayList<Byte> value;

    public ResponseHeader() {
        this.key = null;
        this.value = null;
    }

    public ResponseHeader(String key, ArrayList<Byte> value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public ArrayList<Byte> getValue() {
        return value;
    }
}
