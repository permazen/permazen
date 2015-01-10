
Welcome to JSimpleDB
====================

JSimpleDB is designed to make powerful persistence easy for Java
programmers. It does this by relegating the database to the simplest
role possible - storing data as key/value pairs - and reimplementing
and enhancing all of the usual database supporting features, such
as indexes and notifications, in a Java-centric way. JSimpleDB also
adds a few new features that traditional databases don't provide.

    * Designed from the ground up to be Java-centric; completely type-safe.
    * Scales gracefully to large data sets
    * Configured entirely via Java annotations (of which there are only 12)
    * Regular Java objects and queries - no special "query language" needed
    * Powerful event notification system
    * Support for rolling schema changes across multiple nodes with no downtime
    * Support for indexed fields and user-defined custom types
    * Works on top of any database that can function as a key/value store (SQL, NoSQL, etc.)
    * Extensible command line interface (CLI) supporting arbitrary Java expressions
    * Built-in graphical user interface (GUI) based on Vaadin 

Homepage: https://code.google.com/p/jsimpledb/

Running the Demo
================

This distribution includes a simple example database containing objects in the
solar system. The JSimpleDB command line interface (CLI) and automatic graphical
user interface (GUI) tools will auto-detect the presence of the demonstration
database in the current directory on startup when no other configuration is specified.

The underlying key/value store for the demo database is an XMLKVDatabase instance.
XMLKVDatabase is a sample key/value store implementation that uses a simple flat
XML file format to persist the key/value pairs. This file is demo-database.xml.

To view the demo database in the auto-generated Vaadin GUI:

    java -jar jsimpledb-gui.jar

To view the demo database in the CLI:

    java -jar jsimpledb-cli.jar

$Id$
