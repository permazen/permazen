JSimpleDB makes powerful persistence simple for Java programmers.

JSimpleDB's goal is to make Java persistence as **simple** as possible, doing so in a **Java-centric** manner, while remaining strictly **type safe**.

JSimpleDB does this without sacrificing flexibility or scalability by relegating the database to the simplest role possible - storing data as key/value pairs - and providing all other supporting features, such as indexes, command line interface, etc., in a simpler, type-safe, Java-centric way.

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

Read the [FAQ](FAQ.md) and [Introduction](Introduction.md) for more info, check out the [JPA\_Comparison](JPA_Comparison.md), browse the [Javadocs](http://jsimpledb.googlecode.com/svn/trunk/publish/reports/javadoc/index.html?org/jsimpledb/JSimpleDB.html), or join the [JSimpleDB Users](https://groups.google.com/forum/#!forum/jsimpledb-users) discussion group.

JSimpleDB is a new project (created in 2014) but developing rapidly.