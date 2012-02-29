package de.jbellmann.tomcat.cassandra;

import org.apache.cassandra.thrift.Cassandra.Client;

public interface CassandraCallback {

    Object doInCassandra(Client client) throws RuntimeException;

}
