## JSimpleDB is a better persistence layer for Java

### NOTE: JSimpleDB is currently being renamed to **Permazen**

Persistence is central to most applications. But there are many challenges involved in persistence programming that lie outside of the domain of simply storing the data.

Mainstream Java solutions such as JDBC, JPA and JDO were designed simply to give Java programmers _access_ to existing database functionality. They address the "storage" problem, but leave many other important issues that are inherent to persistence programming poorly addressed, or not addressed at all.

JSimpleDB is a completely different way of looking at persistence programming. Instead of starting from the storage technology side, it starts from the programming language side, asking the simple question, "What are the issues that are inherent to persistence programming, regardless of programming language or database storage technology, and how can they be addressed at the language level in the simplest, most correct, and most language-natural way?"

With JSimpleDB, not only are many issues inherent to persistence programming solved more easily and naturally than before, but also many issues that traditional solutions don't address at all are solved, and some entirely new, useful functionality is added.

JSimpleDB is:

  * A Java persistence layer for SQL, key-value, or in-memory databases
  * A rigorously defined, modular key/value API with adapters for multiple database technologies
  * A way to make your application portable across different database technologies
  * An object serialization framework
  * An automated schema management framework
  * A library for inverting Java references
  * A library for automatic field change notification
  * An embeddable Java command line interface (CLI)

JSimpleDB was inspired by years of frustration with existing persistence solutions, in particular JPA. Compared to using JPA, building an application with JSimpleDB is a refreshingly straightforward experience.

Ask these questions of your persistence solution:

  * **Configuration complexity** Do we have to explicitly configure details of how data is mapped? Are we forced to (ab)use the programming language to address what are really database configuration issues?
  * **Query language concordance** Does the code that performs queries look like regular Java code, or do we have to learn a new “query language”?
  * **Query performance transparency** Is the performance of a query visible and obvious from looking at the code that performs it?
  * **Data type congruence** Are database types equivalent to Java types across the entire domain of values? Are we guaranteed to always read back the same value we write?
  * **First class offline data** Can it be precisely defined which data is copied out of a transaction? Does offline data have all the rights and privileges of “online” (i.e., transactional) data? Does this include the ability to query indexes, and a framework for handling schema differences? Can offline data be easily serialized/deserialized?
  * **Schema verification** Is the schema assumed by the code cross-checked against the schema actually present in the database? Are we always guaranteed a consistent interpretation of stored data?
  * **Incremental schema evolution** Can multiple schemas exist at the same time in the database, to support rolling upgrades? Can data be migrated incrementally, i.e., without stopping the world? Are we free from "whole database" migration operations that would limit scalability?
  * **Structural schema changes** Are structural schema updates performed entirely automatically for us?
  * **Semantic schema changes** Is there a convenient way to specify semantic schema updates, at the language level (not the database level)?
  * **Schema evolution type safety** Is type safety and data type congruence guaranteed across arbitrary schema migrations?
  * **Transactional validation** Does validation, including reference validation, occur only at the end of the transaction (as it should), or randomly and inconveniently in the middle?
  * **Cross-object validation** Is it possible to define validation constraints that span multiple objects/records? Can we register for data-level notifications about changes in non-local objects?
  * **Custom types and indexes** Is it possible to define custom data types, have them be indexed? Is it easy to define arbitrary custom indexes?
  * **Language-level data maintainability** Can database maintenance tasks and queries be performed via a command line interface (CLI) using Java types, values, and expressions (including Java 8 streams and lambdas)? Are there convenient tools for manual and scripted use?
  * **Data store independence** Are we restricted to using only a specific type of database technology, or can virtually any database technology be used by implementing a simple API, making it easy to change later if needed?

JSimpleDB addresses *all* of these issues, this without sacrificing flexibility or scalability.

JSimpleDB does this by treating the database as just a _sorted key/value store_, and implementing the following in Java:

  * Encoding/decoding of field values
  * Referential integrity; forward/reverse delete cascades
  * Indexes (simple and composite)
  * Query views
  * Schema management
  * Change notification
  * Validation queues
  * Command line interface
  * GUI database editor

JSimpleDB also adds several new features that traditional databases don't provide.

### JSimpleDB Slides

For a quick overview, check out these slides from a [JSimpleDB talk](https://s3.amazonaws.com/archie-public/jsimpledb/JSimpleDB-BJUG-Slides2016-05-05.pdf) at a local Java user's group.

### JSimpleDB Paper

For a deeper understanding of the motivation and design decisions behind JSimpleDB, read [JSimpleDB: Language-Driven Persistence for Java](https://cdn.rawgit.com/archiecobbs/jsimpledb/master/jsimpledb-language-driven.pdf).

Abstract:

> Most software applications require durable persistence of data. From a programmer’s point of view, persistence has its own set of inherent issues, e.g., how to manage schema changes, yet such issues are rarely addressed in the programming language itself. Instead, how we program for persistence has traditionally been driven by the storage technology side, resulting in incomplete and/or technology-specific support for managing those issues.

> In Java, the mainstream solution for basic persistence is the Java Persistence API (JPA). While popular, it also measures poorly on how well it addresses many of these inherent issues. We identify several examples, and generalize them into criteria for evaluating how well any solution serves the programmer’s persistence needs, in any language. We introduce JSimpleDB, a persistence layer for ordered key/value stores that, by integrating the data encoding, query, and indexing functions, provides a more complete, type-safe, and language-driven framework for managing persistence in Java, and addresses all of the issues we identify.

### Installing JSimpleDB

JSimpleDB is availble from [Maven Central](http://search.maven.org/#search|ga|1|g%3Aorg.jsimpledb):

```xml
    <dependency>
        <groupId>org.jsimpledb</groupId>
        <artifactId>jsimpledb-main</artifactId>
    </dependency>
```

or from the [Ivy RoundUp](https://github.com/archiecobbs/ivyroundup/) ivy repository:

```xml
<dependency org="org.jsimpledb" name="jsimpledb"/>
```

You should also add the key/value store module(s) for whatever key/value store(s) you want to use, e.g.:

```xml
    <dependency>
        <groupId>org.jsimpledb</groupId>
        <artifactId>jsimpledb-kv-mysql</artifactId>
    </dependency>
```

There is a [demo distribution ZIP file](http://search.maven.org/#search|ga|1|jsimpledb-demo) that lets you play with the JSimpleDB command line and GUI, using a simple database of the solar system.

### Documentation

Documentation and links:

  * [Introduction](https://github.com/archiecobbs/jsimpledb/wiki/Introduction)
  * [Getting Started](https://github.com/archiecobbs/jsimpledb/wiki/GettingStarted)
  * [FAQ](https://github.com/archiecobbs/jsimpledb/wiki/FAQ)
  * [API Javadocs](http://archiecobbs.github.io/jsimpledb/site/apidocs/index.html?org/jsimpledb/JSimpleDB.html)
  * Bullet-point [JPA Comparison](https://github.com/archiecobbs/jsimpledb/wiki/JPA_Comparison)
  * [JSimpleDB Users](https://groups.google.com/forum/#!forum/jsimpledb-users) discussion group
  * Auto-generated [Maven Site](http://archiecobbs.github.io/jsimpledb/site/)
