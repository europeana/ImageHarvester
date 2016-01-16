package eu.europeana.harvester.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class CachingUrlResolver {

    private static final String NO_IP = "127.0.0.1";

    private final LoadingCache<String /* hostname */ , String /* IP */> hostnameToIpCache;

    public CachingUrlResolver() {
        this.hostnameToIpCache =
                CacheBuilder.newBuilder()
                        .maximumSize(10*10000)
                        .expireAfterWrite(24, TimeUnit.HOURS)
                        .build(
                                new CacheLoader<String, String>() {
                                    public String load(String key) throws UnknownHostException {
                                        final InetAddress address = InetAddress.getByName(key);
                                        return address.getHostAddress();
                                    }
                                });
    }


    public final String resolveIpOfUrl(String url) throws MalformedURLException, ExecutionException {
        final String hostname = new URL(url).getHost();
        final String ip = hostnameToIpCache.get(hostname);
        return ip;
    }

    public final String resolveIpOfUrlAndReturnLoopbackOnFail(String url) throws ExecutionException {

        String ipOfUrl = NO_IP;
        try {
            ipOfUrl = resolveIpOfUrl(url);
        } catch (Exception e) {
        }
        return ipOfUrl;

    }

}
