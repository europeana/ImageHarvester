package eu.europeana.crfmigration.domain;

import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;


public class EuropeanaRecord {

    public static Date maximalTimestampUpdated(Collection<EuropeanaRecord> records) {
        Iterator<EuropeanaRecord> recordsIterator = records.iterator();
        if (!recordsIterator.hasNext()) return null; /* Cannot compute minimal date on empty list of dates */
        Date maximalDate = recordsIterator.next().getTimestampUpdated();

        while(recordsIterator.hasNext()) {
            Date recordDate = recordsIterator.next().getTimestampUpdated();
            if (recordDate != null && new DateTime(maximalDate).isBefore(new DateTime(recordDate))) {
                maximalDate = recordDate;
            }
        }

        return maximalDate;
    }

    private final String id;
    private final String about;
    private final String collectionId;
    private final Date timestampUpdated;


    public EuropeanaRecord(String id, String about, String collectionId, Date timestampUpdated) {
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

    public Date getTimestampUpdated() {
        return timestampUpdated;
    }
}
