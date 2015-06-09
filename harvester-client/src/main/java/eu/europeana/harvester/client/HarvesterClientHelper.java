package eu.europeana.harvester.client;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class HarvesterClientHelper {


    public static final Map<String,String> resolveIpsOfUrls(List<String> urls) throws InterruptedException, ExecutionException, TimeoutException {

        int threadPoolSize = 1;
        if (urls.size() > 5 && urls.size() <= 20) threadPoolSize = 3;
        if (urls.size() > 20 && urls.size() <= 100) threadPoolSize = 10;
        if (urls.size() > 100 ) threadPoolSize = 20;

        final ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadPoolSize));

        final List<ListenableFuture<Map.Entry<String, String>>> futures = new ArrayList<ListenableFuture<Map.Entry<String, String>>>();
        for (final String url : urls) {
            futures.add(
                    service.submit(new Callable<Map.Entry<String, String>>() {
                        public Map.Entry<String, String> call() {
                            try {
                                final InetAddress address = InetAddress.getByName(new URL(url).getHost());
                                return Maps.immutableEntry(url, address.getHostAddress());

                            } catch (Exception e) {
                                return Maps.immutableEntry(url, null);
                            }
                        }
                    }));
        }

        final Map<String,String> results = new HashMap();
        for (final Map.Entry<String,String> entry : Futures.allAsList(futures).get(1, TimeUnit.MINUTES)) {
            results.put(entry.getKey(),entry.getValue());
        }

        return results;

    }


}
