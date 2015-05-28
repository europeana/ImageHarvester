package eu.europeana.harvester.utils;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

public class NetUtils {

    public static final String ipOfUrl(final String url) throws MalformedURLException, UnknownHostException {
        final InetAddress address = InetAddress.getByName(new URL(url).getHost());
        return address.getHostAddress();
    }
}
