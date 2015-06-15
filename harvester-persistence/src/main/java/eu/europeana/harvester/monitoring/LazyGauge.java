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

    public boolean registerHandler(final Gauge<Integer> handler) {
        if (gauge == null) {
            gauge = metricRegistry.register(metricRegistry.
                            name(LazyGauge.class, name),
                    new Gauge<Integer>() {
                        @Override
                        public Integer getValue() {
                            return handler.getValue();
                        }
                    });
            return true;
        } else {
            return false;
        }
    }

}
