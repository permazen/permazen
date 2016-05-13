
Welcome to JSimpleDB
====================

JSimpleDB is a better persistence layer for Java

Mainstream persistence solutions such as JPA and JDO fail to address many important issues that are _inherent_ to persistence programming. This is because they were not designed to address these issues; they were designed merely to give Java programmers access to existing database functionality.

In particular:

  * [Configuration complexity] Do we have to explicitly configure how data is mapped? Are we forced to (ab)use the programming language to address what are really database configuration issues?
  * [Query language concordance] Does the code that performs queries look like regular Java code, or do we have to learn a new “query language”?
  * [Query performance transparency] Is the performance of a query obvious from looking at the code that performs it?
  * [Data type congruence] Do database types agree with Java types? Are all field values supported? Do we always read back the same values we write?
  * [First class offline data] Can it be precisely defined which data is actually copied out of a transaction? Does offline data have all the rights and privileges of “online” (i.e., transactional) data? Does this include index queries and a framework for handling schema differences?
  * [Schema verification] Is the schema assumed by the code cross-checked against the schema actually present in the database?
  * [Incremental schema evolution] Can multiple schemas exist at the same time in the database, to support rolling upgrades? Can data be migrated incrementally, i.e., without stopping the world? Are any whole database operations ever required?
  * [Structural schema changes] Are structural schema updates performed automatically?
  * [Semantic schema changes] Is there a convenient way to specify semantic schema updates, preferably at the Java level, not the database level? Do semantic updates have access to both the old and the new values?
  * [Schema evolution type safety] Is type safety and data type congruence guaranteed across arbitrary schema migrations?
  * [Transactional validation] Does validation only occur at the end of the transaction, or randomly and inconveniently in the middle?
  * [Cross-object validation] Is it possible to define validation constraints that span multiple objects/records?
  * [Language-level data maintainability] Can data maintenance tasks be performed using the normal Java types and values? Are there convenient tools for manual and scripted use?

JSimpleDB addresses all of these issues, this without sacrificing flexibility or scalability.

JSimpleDB does this by treating the database as just a sorted key/value store, and implementing the following in Java:

  * Encoding/decoding of field values
  * Enforcing referential integrity; forward/reverse delete cascades
  * Field indexes (simple and composite)
  * Query views
  * Schema management
  * Change notification
  * Validation queues
  * Command line interface

JSimpleDB also adds some new features that traditional databases don't provide.

  * Designed from the ground up to be Java-centric; 100% type-safe at all times.
  * Works on top of any database that can function as a key/value store (SQL, NoSQL, etc.)
  * Scales gracefully to large data sets; no "whole database" operation is ever required
  * Configured entirely via Java annotations (only one is required)
  * Queries are regular Java code - there is no "query language" needed
  * Change notifications from arbitrarily distant objects
  * Built-in support for rolling schema changes across multiple nodes with no downtime
  * Supports simple and composite indexes, including on user-defined custom types
  * Extensible command line interface (CLI) including Java 8 expression parser
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
"expr" command evaluates any Java 8 expression and is used for database queries.
For example, to query for all Moon objects, enter "expr all(Moon)".

The "expr" command supports several extensions to Java syntax, including:

    * Functions may appear (the "all()" function used above is an example)
    * Objects may be referred to by object ID literal, eg., @fc02ac0000000001
    * Session variables have the form $NAME; you can set them and use them later

