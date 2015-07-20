package eu.europeana.harvester.cluster.master.reloader;

import org.joda.time.Duration;

/**
 * Created by salexandru on 20.07.2015.
 */
public class JobReloaderConfig {
    private final Duration numberOfSeconds;

    public JobReloaderConfig (Duration numberOfSeconds) {this.numberOfSeconds = numberOfSeconds;}

    public Duration getNumberOfSeconds () {
        return numberOfSeconds;
    }
}
