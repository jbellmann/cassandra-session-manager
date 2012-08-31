package de.jbellmann.tomcat.cassandra.example.controller;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class ControllerIT {

    @Before
    public void setUp() throws InterruptedException {
        System.out.println("Lets wait 20 seconds to start all tomcats");
        Thread.sleep(20000);
    }

    @Test
    public void testServlet() throws ClientProtocolException, IOException, InterruptedException {
        DefaultHttpClient client = new DefaultHttpClient();
        HttpGet method = new HttpGet("http://localhost:9090/cassandra-example/add/");
        HttpGet method2 = new HttpGet("http://localhost:9091/cassandra-example/add/");
        HttpGet method3 = new HttpGet("http://localhost:9092/cassandra-example/add/");
        HttpGet method4 = new HttpGet("http://localhost:9093/cassandra-example/add/");
        HttpGet method5 = new HttpGet("http://localhost:9094/cassandra-example/add/");
        List<HttpGet> getMethodList = Lists.newArrayList();
        getMethodList.add(method2);
        getMethodList.add(method);
        getMethodList.add(method5);
        getMethodList.add(method4);
        getMethodList.add(method3);
        Iterator<HttpGet> endless = Iterators.cycle(getMethodList);
        int invocations = 12;
        while(invocations > 0){
            invocations--;
            client.execute(endless.next(), handler);
            Thread.sleep(2000);
        }
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
