package de.jbellmann.tomcat.cassandra;

import java.io.Serializable;
import java.util.UUID;

public class MySession implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;

    public MySession() {
        // defaultConstructor
    }

    public static MySession create() {
        MySession session = new MySession();
        session.id = UUID.randomUUID().toString().replace("-", "");
        return session;
    }

    public String getId() {
        return this.id;
    }

    @Override
    public String toString() {
        return "MySession[id=" + id + "]";
    }

}
