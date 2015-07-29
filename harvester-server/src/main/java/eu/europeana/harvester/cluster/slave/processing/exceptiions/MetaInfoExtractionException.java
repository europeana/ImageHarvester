package eu.europeana.harvester.cluster.slave.processing.exceptiions;

/**
 * Created by salexandru on 29.07.2015.
 */
public class MetaInfoExtractionException extends RuntimeException {
    public MetaInfoExtractionException (Exception e) {
        super(e);
    }
}
