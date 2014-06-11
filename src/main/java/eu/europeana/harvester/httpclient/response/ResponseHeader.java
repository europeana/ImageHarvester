package eu.europeana.harvester.httpclient.response;

import java.io.Serializable;
import java.util.ArrayList;

public class ResponseHeader implements Serializable {

    private final String key;

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
