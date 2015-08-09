package eu.europeana.harvester.monitoring;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * Allows to declare a gauge for which the handler can be declared later.
 * Useful to declare the gauge static but initialised/used in a non-static context.
 */
public class LazyGauge {
    private final MetricRegistry metricRegistry;
    private final String name;
    private Gauge gauge = null;

    public LazyGauge(MetricRegistry metricRegistry, String name) {
        this.metricRegistry = metricRegistry;
        this.name = name;
    }

    public <T> boolean registerHandler(final Gauge<T> handler) {
        if (gauge == null) {
            gauge = metricRegistry.register(metricRegistry.name(name), handler);
            return true;
        } else {
            return false;
        }
    }


}
