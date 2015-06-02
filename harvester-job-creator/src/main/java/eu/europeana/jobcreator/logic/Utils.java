package eu.europeana.jobcreator.logic;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;

public class Utils {
    /**
     * Gets the ip address of a source document.
     *
     * @param url the resource url
     * @return - ip address
     */
    public static final String ipAddressOf(final String url) throws UnknownHostException, MalformedURLException {
        return InetAddress.getByName(new URL(url).getHost()).getHostAddress();
    }

}
