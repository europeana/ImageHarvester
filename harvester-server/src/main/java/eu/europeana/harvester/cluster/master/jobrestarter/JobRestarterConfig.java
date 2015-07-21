package eu.europeana.harvester.cluster.master.jobrestarter;

import com.typesafe.config.Config;
import org.joda.time.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Created by salexandru on 20.07.2015.
 */
public class JobRestarterConfig {
    private final Duration numberOfSecondsBetweenRepetition;

    public JobRestarterConfig (Duration numberOfSecondsBetweenRepetition) {this.numberOfSecondsBetweenRepetition = numberOfSecondsBetweenRepetition;}

    public Duration getNumberOfSecondsBetweenRepetition () {
        return numberOfSecondsBetweenRepetition;
    }

    public static JobRestarterConfig valueOf (final Config config) {
        return new JobRestarterConfig(new Duration(config.getDuration("jobRestarterTimeBetweenRepetitions", TimeUnit.SECONDS)));
    }
}
