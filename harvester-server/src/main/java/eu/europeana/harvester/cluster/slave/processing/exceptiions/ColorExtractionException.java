package eu.europeana.harvester.cluster.slave.processing.exceptiions;

/**
 * Created by salexandru on 29.07.2015.
 */
public class ColorExtractionException extends  RuntimeException {
    public ColorExtractionException (Exception e) {
       super(e);
    }
}
