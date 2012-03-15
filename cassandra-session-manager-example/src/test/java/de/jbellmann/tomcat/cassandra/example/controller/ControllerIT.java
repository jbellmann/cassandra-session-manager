package de.jbellmann.tomcat.cassandra.example.controller;

import java.io.IOException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

public class ControllerIT {

    @Test
    public void testServlet() throws ClientProtocolException, IOException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet method = new HttpGet("http://localhost:9090/cassandra-example/add/");
        client.execute(method, handler);
        client.execute(method, handler);
        client.execute(method, handler);
        client.execute(method, handler);
    }

    // Just to consume response
    private final ResponseHandler<byte[]> handler = new ResponseHandler<byte[]>() {
        public byte[] handleResponse(HttpResponse response) throws ClientProtocolException, IOException {
            System.out.println("HttpStatus: " + response.getStatusLine().getStatusCode());
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                return EntityUtils.toByteArray(entity);
            } else {
                return null;
            }
        }
    };
}
