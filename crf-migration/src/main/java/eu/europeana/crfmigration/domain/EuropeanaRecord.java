package eu.europeana.crfmigration.domain;

import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;


public class EuropeanaRecord {

    public static DateTime minimalTimestampUpdated(Collection<EuropeanaRecord> records) {
        Iterator<EuropeanaRecord> recordsIterator = records.iterator();
        if (!recordsIterator.hasNext()) return null; /* Cannot compute minimal date on empty list of dates */
        DateTime minimalDate = recordsIterator.next().getTimestampUpdated();

        while(recordsIterator.hasNext()) {
            DateTime recordlDate = recordsIterator.next().getTimestampUpdated();
            if (recordlDate != null && minimalDate.isAfter(recordlDate)) {
                minimalDate = recordlDate;
            }
        }

        return minimalDate;
    }

    private final String id;
    private final String about;
    private final String collectionId;
    private final DateTime timestampUpdated;


    public EuropeanaRecord(String id, String about, String collectionId, DateTime timestampUpdated) {
        this.id = id;
        this.about = about;
        this.collectionId = collectionId;
        this.timestampUpdated = timestampUpdated;
    }

    public String getId() {
        return id;
    }

    public String getAbout() {
        return about;
    }

    public String getCollectionId() {
        return collectionId;
    }

    public DateTime getTimestampUpdated() {
        return timestampUpdated;
    }
}
