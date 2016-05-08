
Welcome to JSimpleDB
====================

JSimpleDB makes powerful persistence simple for Java programmers.

JSimpleDB's goal is to make Java persistence as simple as possible, doing so in
a Java-centric manner, while remaining strictly type safe.

JSimpleDB does this without sacrificing flexibility or scalability by relegating
the database to the simplest role possible - storing data as key/value pairs -
and providing all other supporting features, such as indexes, command line
interface, etc., in a simpler, type-safe, Java-centric way.

JSimpleDB also adds important new features that traditional databases don't provide.

    * Designed from the ground up to be Java-centric; completely type-safe.
    * Works on top of any database that can function as a key/value store (SQL, NoSQL, etc.)
    * Scales gracefully to large data sets; no "whole database" operation is ever required
    * Configured entirely via Java annotations (only one is required)
    * Queries are regular Java code - there is no "query language" needed
    * Change notifications from arbitrarily distant objects
    * Built-in support for rolling schema changes across multiple nodes with no downtime
    * Supports simple and composite indexes and user-defined custom types
    * Extensible command line interface (CLI) supporting arbitrary Java queries
    * Built-in Java-aware graphical user interface (GUI) based on Vaadin 

Homepage: https://github.com/archiecobbs/jsimpledb/

Running the Demo
================

This distribution includes a simple example database containing objects in the
solar system. The JSimpleDB command line interface (CLI) and automatic graphical
user interface (GUI) tools will auto-detect the presence of the demonstration
database in the current directory on startup when no other configuration is specified.

The demo-classes/ subdirectory contains the Java model classes used to create the
example database. The database and GUI are entirely configured from the handful
of annotations on these classes. Note that you are free to put these annotations
anywhere in the type hierarchy (see for example Body.java and AbstractBody.java).

The underlying key/value store for the demo database is an XMLKVDatabase instance.
XMLKVDatabase is a "toy" key/value store implementation that uses a simple flat
XML file format for persisting the key/value pairs. That file is demo-database.xml.

The database was originally loaded using the "load" command in the CLI, which reads
files in "object XML" format (in this case, the file was "demo-objs.xml").

To view the demo database in the auto-generated Vaadin GUI:

    java -jar jsimpledb-gui.jar

Then connect to port 8080. If you already have something running on port 8080,
add the "--port XXXX" flag. Add the "--help" flag to see other options.

To view the demo database in the CLI:

    java -jar jsimpledb-cli.jar

Use the "help" command to see all commands and functions. In particular, the
"expr" command evaluates any Java expression and is used for database queries.
For example, to query for all Moon objects, enter "expr all(Moon)".

The "expr" command supports several extensions to Java syntax, including:

    * Functions may appear (the "all()" function used above is an example)
    * Objects may be referred to by object ID literal, eg., @fc02ac0000000001
    * Session variables have the form $NAME; you can set them and use them later

