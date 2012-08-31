package de.jbellmann.tomcat.cassandra.astyanax;

import de.jbellmann.tomcat.cassandra.CassandraManager;
import de.jbellmann.tomcat.cassandra.CassandraTemplate;

/**
 * 
 * @author Joerg Bellmann
 *
 */
public class AstyanaxCassandraManager extends CassandraManager {

    private static final String INFO = AstyanaxCassandraManager.class.getName() + "/1.0";
    
    @Override
    public CassandraTemplate createCassandraTemplate() {
        return new AstyanaxCassandraTemplate();
    }
    

    @Override
    public String getName() {
        return "astyanax-cassandra-manager";
    }

    @Override
    public String getInfo() {
        return INFO;
    }

}
