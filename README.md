JSimpleDB makes persistence simpler and more natural for Java programmers.

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

Read the [Introduction](https://github.com/archiecobbs/jsimpledb/wiki/Introduction), [GettingStarted](https://github.com/archiecobbs/jsimpledb/wiki/GettingStarted), and the [FAQ](https://github.com/archiecobbs/jsimpledb/wiki/FAQ) for more info, browse the comprehensive [Javadocs](http://archiecobbs.github.io/jsimpledb/publish/reports/javadoc/index.html?org/jsimpledb/JSimpleDB.html), check out the [JPA\_Comparison](https://github.com/archiecobbs/jsimpledb/wiki/JPA_Comparison), or join the [JSimpleDB Users](https://groups.google.com/forum/#!forum/jsimpledb-users) discussion group.

The paper [JSimpleDB: Language-Natural Persistence for Java](https://cdn.rawgit.com/archiecobbs/jsimpledb/master/paper.pdf) descrives the issues that are inherent to persitence programming and how JSimpleDB addresses them. Abstract:

> Most software applications require durable persistence of data. From a programmer’s point of view, persistence has its own set of inherent issues, e.g., how to manage schema changes, yet such issues are rarely addressed in the programming language itself. Instead, how we program for persistence has traditionally been driven by the storage technology side, resulting in incomplete and/or technology-specific support for managing those issues.

> In Java, the mainstream solution for basic persistence is the Java Persistence API (JPA). While popular, it also measures poorly on how well it addresses many of these inherent issues. We identify several examples, and generalize them into criteria for evaluating how well any solution serves the programmer’s persistence needs, in any language. We introduce JSimpleDB, a persistence layer for ordered key/value stores that, by integrating the data encoding, query, and indexing functions, provides a more complete, type-safe, and language-driven framework for managing persistence in Java, and addresses all of the issues we identify.
