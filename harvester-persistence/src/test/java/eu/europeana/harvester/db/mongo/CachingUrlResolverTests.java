package eu.europeana.harvester.db.mongo;


import eu.europeana.harvester.util.CachingUrlResolver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertNotNull;

public class CachingUrlResolverTests {

    @Rule
    public Timeout globalTimeout = new Timeout(1000); // 1 second max per method tested

    @Test
    public void canResolve1MillionTimesTheSameIpInUnder1Second() throws InterruptedException, ExecutionException, TimeoutException, MalformedURLException {

        final String url1 = "http://stackoverflow.com/";
        final CachingUrlResolver cache = new CachingUrlResolver();
        for (int i = 0; i< 1000*000;i++) {
            assertNotNull(cache.resolveIpOfUrl(url1));
        }

    }

}
