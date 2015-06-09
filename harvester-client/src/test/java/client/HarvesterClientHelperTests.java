package client;


import eu.europeana.harvester.client.HarvesterClientHelper;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class HarvesterClientHelperTests {

    @Test
    public void canResolveIpsConcurrently() throws InterruptedException, ExecutionException, TimeoutException {

        final String url1 = "http://stackoverflow.com/";
        final String url2 = "http://edition.cnn.com/";
        final String url3 = "http://www.bbc.com/";

        final Map<String,String> results = HarvesterClientHelper.resolveIpsOfUrls(Arrays.asList(url1, url2, url3));

        assertNotNull(results.get(url1));
        assertNotNull(results.get(url2));
        assertNotNull(results.get(url3));

    }
}
