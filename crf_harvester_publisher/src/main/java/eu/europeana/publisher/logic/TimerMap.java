package eu.europeana.publisher.logic;

import com.codahale.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by salexandru on 25.06.2015.
 */
public class TimerMap {
    private final String id;

    public TimerMap(final String id) {
       this.id = id;
    }

    public Timer time(final String id) {
        final String metricName = name(this.id, id, PublisherMetrics.DURATION);

        return PublisherMetrics.METRIC_REGISTRY.timer(metricName);
    }
}

