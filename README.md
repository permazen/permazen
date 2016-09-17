### JSimpleDB is a better persistence layer for Java

Mainstream persistence solutions such as JPA and JDO fail to address many important issues that are _inherent_ to persistence programming. This is because they were not designed to address these issues; they were designed merely to give Java programmers access to existing database functionality.

In particular:

  * **Configuration complexity** Do we have to explicitly configure how data is mapped? Are we forced to (ab)use the programming language to address what are really database configuration issues?
  * **Query language concordance** Does the code that performs queries look like regular Java code, or do we have to learn a new “query language”?
  * **Query performance transparency** Is the performance of a query obvious from looking at the code that performs it?
  * **Data type congruence** Do database types agree with Java types? Are all field values supported? Do we always read back the same values we write?
  * **First class offline data** Can it be precisely defined which data is actually copied out of a transaction? Does offline data have all the rights and privileges of “online” (i.e., transactional) data? Does this include index queries and a framework for handling schema differences? Can offline data be easily serialized/deserialized?
  * **Schema verification** Is the schema assumed by the code cross-checked against the schema actually present in the database?
  * **Incremental schema evolution** Can multiple schemas exist at the same time in the database, to support rolling upgrades? Can data be migrated incrementally, i.e., without stopping the world? Are any whole database operations ever required?
  * **Structural schema changes** Are structural schema updates performed automatically?
  * **Semantic schema changes** Is there a convenient way to specify semantic schema updates, preferably at the Java level, not the database level? Do semantic updates have access to both the old and the new values?
  * **Schema evolution type safety** Is type safety and data type congruence guaranteed across arbitrary schema migrations?
  * **Transactional validation** Does validation only occur at the end of the transaction, or randomly and inconveniently in the middle?
  * **Cross-object validation** Is it possible to define validation constraints that span multiple objects/records?
  * **Language-level data maintainability** Can data maintenance tasks be performed using the normal Java types and values? Are there convenient tools for manual and scripted use?

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

### JSimpleDB Paper

The paper [JSimpleDB: Language-Driven Persistence for Java](https://cdn.rawgit.com/archiecobbs/jsimpledb/master/jsimpledb-language-driven.pdf) describes the issues that are inherent to persistence programming and how JSimpleDB addresses them.

Abstract:

> Most software applications require durable persistence of data. From a programmer’s point of view, persistence has its own set of inherent issues, e.g., how to manage schema changes, yet such issues are rarely addressed in the programming language itself. Instead, how we program for persistence has traditionally been driven by the storage technology side, resulting in incomplete and/or technology-specific support for managing those issues.

> In Java, the mainstream solution for basic persistence is the Java Persistence API (JPA). While popular, it also measures poorly on how well it addresses many of these inherent issues. We identify several examples, and generalize them into criteria for evaluating how well any solution serves the programmer’s persistence needs, in any language. We introduce JSimpleDB, a persistence layer for ordered key/value stores that, by integrating the data encoding, query, and indexing functions, provides a more complete, type-safe, and language-driven framework for managing persistence in Java, and addresses all of the issues we identify.

Also available are slides from a [JSimpleDB talk](https://s3.amazonaws.com/archie-public/jsimpledb/JSimpleDB-BJUG-Slides2016-05-05.pdf) at a local Java user's group.

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

Read the [Introduction](https://github.com/archiecobbs/jsimpledb/wiki/Introduction), [GettingStarted](https://github.com/archiecobbs/jsimpledb/wiki/GettingStarted), and the [FAQ](https://github.com/archiecobbs/jsimpledb/wiki/FAQ) for more info, browse the comprehensive [Javadocs](http://archiecobbs.github.io/jsimpledb/site/apidocs/index.html?org/jsimpledb/JSimpleDB.html), check out the [JPA\_Comparison](https://github.com/archiecobbs/jsimpledb/wiki/JPA_Comparison), or join the [JSimpleDB Users](https://groups.google.com/forum/#!forum/jsimpledb-users) discussion group.

### Maven Site

The auto-generated Maven site is [here](http://archiecobbs.github.io/jsimpledb/site/).
