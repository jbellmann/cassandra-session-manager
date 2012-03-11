package de.jbellmann.tomcat.cassandra;

import org.apache.catalina.Manager;

/**
 * 
 * @author Joerg Bellmann
 *
 */
public class SimpleCassandraSession extends CassandraSession {

    public SimpleCassandraSession(Manager manager) {
        super(manager);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void expire() {
    }

}
