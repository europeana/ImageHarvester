package eu.europeana.harvester.httpclient.request;

import org.jboss.netty.handler.codec.http.*;

import java.net.URL;
import java.util.Map;

/**
 * Helpers to create HTTP netty requests from URL's.
 */
public class HttpHEAD {

    public static HttpRequest build(final URL url) {
        final HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.HEAD, url.toString());
        request.headers().set(HttpHeaders.Names.HOST, url.getHost());
        request.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.CLOSE);
        request.headers().set(HttpHeaders.Names.ACCEPT_ENCODING, HttpHeaders.Values.GZIP);
        return request;
    }

    public static HttpRequest build(final URL url,Map<String,String> cookies) {
        final HttpRequest request = build(url);
        final CookieEncoder httpCookieEncoder = new CookieEncoder(false);
        for (Map.Entry<String,String> cookieEntry : cookies.entrySet()) {
            httpCookieEncoder.addCookie(cookieEntry.getKey(),cookieEntry.getValue());
        }
        request.headers().set(HttpHeaders.Names.COOKIE, httpCookieEncoder.encode());
        return request;
    }
}
