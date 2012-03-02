package de.jbellmann.tomcat.cassandra;

import org.apache.cassandra.thrift.Cassandra.Client;

public interface CassandraCallback<T> {

    T doInCassandra(Client client) throws RuntimeException;

}
