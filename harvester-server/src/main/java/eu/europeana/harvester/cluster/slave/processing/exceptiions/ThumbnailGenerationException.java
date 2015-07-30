package eu.europeana.harvester.cluster.slave.processing.exceptiions;

/**
 * Created by salexandru on 29.07.2015.
 */
public class ThumbnailGenerationException extends RuntimeException {
    public ThumbnailGenerationException (Exception e) {super(e);}
}
