package eu.europeana.publisher.logic;

import com.codahale.metrics.Counter;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by salexandru on 21.08.2015.
 */
public class CounterMap {
   private final String id;

   public  CounterMap (final String id) {
      this.id = id;
   }

   public void inc(final String name, final long value) {
      final String counterName = name(this.id, id, PublisherMetrics.COUNTER);

      PublisherMetrics.METRIC_REGISTRY.counter(counterName).inc(value);
   }

   public void dec(final String name, final long value) {
      final String counterName = name(this.id, id, PublisherMetrics.COUNTER);

      PublisherMetrics.METRIC_REGISTRY.counter(counterName).dec(value);
   }
}
