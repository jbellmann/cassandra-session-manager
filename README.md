Cassandra Session Manager
=========================

Is a Tomcat Session Manager Implementation that uses Apache Cassandra (NoSQL)-DB as an object store.

How to build
============

This Project uses Maven as Build-System.

With Maven installed you can run
´´´´
mvn install
´´´´
to build the project.

To run the example application too, specify a profile
´´´´
mvn install -PwithExample
´´´´

How to configure
================

In your Webapplication create an folder _META-INF_ where you put a file _context.xml_ with a _Manager_-declaration

	<?xml version="1.0" encoding="UTF-8"?>
	<Context>
	  <Manager className="de.jbellmann.tomcat.cassandra.CassandraManager"
	           hosts="localhost:9160"
	           maxActiveConnections="5"
	           logSessionsOnStartup="true"/>
	</Context>
