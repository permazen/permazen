## Permazen is a different persistence layer for Java

Permazen is a ground-up rethink of Java persistence. It has been in production use in commercial software since 2015.

### What is it?

Permazen is:

  * A Java persistence layer for SQL, key-value, or in-memory databases
  * A rigorously defined, modular key/value API with adapters for 15+ database technologies
  * An object serialization framework
  * An automated schema management framework
  * A library for inverting Java references
  * A library for precise, non-local field change notifications
  * An embeddable Java command line interface (CLI)

Got five minutes? Read [Persistence Programming: Are We Doing This Right?](https://queue.acm.org/detail.cfm?id=3526210) in the Jan/Feb 2022 issue of ACM Queue magazine for an overview of the Permazen "story".

### What's the motivation?

Persistence is central to most applications. But there are many challenges involved in persistence programming that lie outside of the domain of simply storing the data.

Mainstream Java solutions such as JDBC, JPA and JDO were designed simply to give Java programmers _access_ to existing database functionality. They address the "storage" problem, but leave many other important issues that are inherent to persistence programming poorly addressed, or not addressed at all.

Permazen is a completely different way of looking at persistence programming. Instead of starting from the storage technology side, it starts from the programming language side, asking the simple question, "What are the issues that are inherent to persistence programming, regardless of programming language or database storage technology, and how can they be addressed at the language level in the simplest, most correct, and most language-natural way?"

With Permazen, not only are many issues inherent to persistence programming solved more easily and naturally than before, but also many issues that traditional solutions don't address at all are solved, and some entirely new, useful functionality is added.

Permazen was inspired by years of frustration with existing persistence solutions, in particular JPA. Compared to using JPA, building an application with Permazen is a refreshingly straightforward experience.

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

Permazen attempts to address all of these issues. It does so by treating the database as just a _sorted key/value store_ and implementing everything else in Java:

  * Encoding/decoding of field values
  * Referential integrity; forward/reverse cascades
  * Indexes (simple and composite)
  * Query views
  * Schema management
  * Change notification
  * Validation queues
  * "Offline" data
  * Command line interface
  * GUI database editor

Permazen also adds several new features that traditional databases don't provide. For example, you can define your own basic types and then index them however you want.

### What's the Downside?

Permazen redefines the "line of demarcation" between a Java application and its supporting database. However, this has some implications:

**For an equivalent query, Permazen will perform more frequent, but smaller, database accesses.** As a result, in situations where the code and the data are separated by a high latency network, Permazen will probably be too slow. Permazen is best suited for scenarios where the code and the data are running on the same machine or in close proximity.

**You have to learn something new.** Persistence programming with Permazen requires a different way of thinking. For example, a "DAO" layer is often no longer necessary, and you may have to think harder about how to query your data efficiently, instead of crossing your fingers and hoping the database figures it out for you, because there is no [query planner](https://en.wikipedia.org/wiki/Query_plan) (_you_ are the query planner).

**More flexible type hiearchies are possible, but it's also easy to make a mess.** JPA has support for class inheritance and only partial support for generics. Permazen supports interface inheritance (including Java's equivalent of "mix-ins") and fully supports generic types. The restrictions imposed by JPA tend to force model classes to stay simpler. With Permazen, implementing an interface (directly or indirectly) can mean that a model class inherits a bunch of new persistent fields.

### What are some interesting Java classes to look at?

*Key/Value Layer*

  * [`KVStore`](https://permazen.github.io/permazen/site/apidocs/io/permazen/kv/KVStore.html) - A thing that contains key/value pairs
  * [`KVDatabase`](https://permazen.github.io/permazen/site/apidocs/io/permazen/kv/KVDatabase.html) - A thing that persists key/value pairs
  * [`KVTransaction`](https://permazen.github.io/permazen/site/apidocs/io/permazen/kv/KVTransaction.html) - A transaction for a `KVDatabase`
  * [`BranchedKVTransaction`](https://permazen.github.io/permazen/site/apidocs/io/permazen/kv/mvcc/BranchedKVTransaction.html) - An in-memory transaction that reattaches on commit
  * [`RaftKVDatabase`](https://permazen.github.io/permazen/site/apidocs/io/permazen/kv/raft/RaftKVDatabase.html) - A distributed `KVDatabase` based on the [Raft consensus algorithm](https://raft.github.io/).

*Core API Layer*

  * [`Database`](https://permazen.github.io/permazen/site/apidocs/io/permazen/core/Database.html) - A Permazen core API database instance
  * [`Transaction`](https://permazen.github.io/permazen/site/apidocs/io/permazen/core/Transaction.html) - A Permazen core API database transaction
  * [`Encoding`](https://permazen.github.io/permazen/site/apidocs/io/permazen/encoding/Encoding.html) - Defines how simple database types are encoded/decoded

*Java Layer*

  * [`Permazen`](https://permazen.github.io/permazen/site/apidocs/io/permazen/Permazen.html) - A Permazen database instance
  * [`PermazenTransaction`](https://permazen.github.io/permazen/site/apidocs/io/permazen/PermazenTransaction.html) - A Permazen database transaction
  * [`PermazenObject`](https://permazen.github.io/permazen/site/apidocs/io/permazen/PermazenObject.html) - Interface implemented by runtime-generated concrete model classes
  * [`@PermazenType`](https://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/PermazenType.html) - Annotation identifying your database classes
  * [`@PermazenField`](https://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/PermazenField.html) - Annotation configuring your database fields

*Other*

  * [`@OnChange`](https://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/OnChange.html) - How change notifications are delivered
  * [`ReferencePath`](https://permazen.github.io/permazen/site/apidocs/io/permazen/ReferencePath.html) - Describes a path between objects that hops through one or more forward and/or inverse references
  * [`@OnSchemaChange`](https://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/OnSchemaChange.html) - How schema update "fixups" are defined
  * [`PermazenObjectHttpMessageConverter`](https://permazen.github.io/permazen/site/apidocs/io/permazen/spring/PermazenObjectHttpMessageConverter.html) - For sending/receiving versioned graphs of objects over the network using Spring

### How are Java object, data structures, and indexes mapped into key/value pairs?

See [`LAYOUT.md`](https://github.com/permazen/permazen/blob/master/LAYOUT.md).

### Permazen Article

[Persistence Programming: Are We Doing This Right?](https://queue.acm.org/detail.cfm?id=3526210) appears in the Jan/Feb 2022 issue of ACM Queue magazine. This gives a good overview of how Permazen tries to improve the persistence programming experience.

### Permazen Slides

For a quick overview, check out these slides from a [JSimpleDB talk](https://s3.amazonaws.com/archie-public/jsimpledb/JSimpleDB-BJUG-Slides2016-05-05.pdf) at a local Java user's group (Permazen was previously named JSimpleDB).

### Permazen Paper

For a deeper understanding of the motivation and design decisions behind Permazen, read [Permazen: Language-Driven Persistence for Java](https://cdn.rawgit.com/permazen/permazen/master/permazen-language-driven.pdf).

Abstract:

> Most software applications require durable persistence of data. From a programmer’s point of view, persistence has its own set of inherent issues, e.g., how to manage schema changes, yet such issues are rarely addressed in the programming language itself. Instead, how we program for persistence has traditionally been driven by the storage technology side, resulting in incomplete and/or technology-specific support for managing those issues.

> In Java, the mainstream solution for basic persistence is the Java Persistence API (JPA). While popular, it also measures poorly on how well it addresses many of these inherent issues. We identify several examples, and generalize them into criteria for evaluating how well any solution serves the programmer’s persistence needs, in any language. We introduce Permazen, a persistence layer for ordered key/value stores that, by integrating the data encoding, query, and indexing functions, provides a more complete, type-safe, and language-driven framework for managing persistence in Java, and addresses all of the issues we identify.

### Installing Permazen

Permazen is available from [Maven Central](https://search.maven.org/#search|ga|1|g%3Aio.permazen):

```xml
    <dependency>
        <groupId>io.permazen</groupId>
        <artifactId>permazen-main</artifactId>
    </dependency>
```

You should also add the key/value store module(s) for whatever key/value store(s) you want to use, e.g.:

```xml
    <dependency>
        <groupId>io.permazen</groupId>
        <artifactId>permazen-kv-sqlite</artifactId>
    </dependency>
```

There is a [demo distribution ZIP file](https://search.maven.org/#search|ga|1|permazen-demo) that lets you play with the Permazen command line.

### Maven Plugin

The Permazen Maven plugin includes a `verify-schema` goal. This goal compares the Permazen schema generated from your Java model classes to an expected reference schema, and fails the build if there are any differences.

A change in your schema is not a bad thing, but it does mean you should double-check that (a) the schema change was actually intended, and (b) that you've added any new [`@OnSchemaChange`](https://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/OnSchemaChange.html) logic that may be needed.

All plugin configuration settings are optional; i.e., the plugin will run just fine using its default configuration.

By default, it scans every class in `${project.build.directory}` looking for [`@PermazenType`](https://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/PermazenType.html) classes. Or you can tell it where to find your model classes via `<packages>` and/or `<classes>`. This will be required if you have multiple executions for multiple schemas, e.g., one for the database and another for serialized network messaging.

```xml
    <!-- Permazen schema verification -->
    <plugin>
        <groupId>io.permazen</groupId>
        <artifactId>permazen-maven-plugin</artifactId>
        <version>${permazen.version}</version>
        <executions>
            <execution>
                <goals>
                    <goal>verify-schema</goal>
                </goals>
                <configuration>

                    <!-- Where to find my model classes -->
                    <packages>
                        <package>com.example.myapp.model</package>
                    </packages>

                    <!-- Or specify individual class names -->
                    <classes>
                        <class>com.example.myapp.model.ModelClassA</class>
                        <class>com.example.myapp.model.ModelClassB</class>
                    </classes>

                    <!-- If you have any custom field encodings, specify your EncodingRegistry -->
                    <encodingRegistryClass>com.example.myapp.encoding.MyEncodingRegistry</encodingRegistryClass>

                    <!-- Optionally store the generated schema's unique schema ID in a property -->
                    <schemaIdProperty>my.schema.id</schemaIdProperty>

                    <!-- Configuration for expected and actual schema XML files (default value shown) -->
                    <expectedSchemaFile>${basedir}/src/main/permazen/expected-schema.xml</expectedSchemaFile>
                    <actualSchemaFile>${project.build.directory}/schema.xml</actualSchemaFile>

                    <!-- Other rarely used settings (default value shown) -->
                    <oldSchemasDirectory>${basedir}/src/main/permazen/old</oldSchemasDirectory>
                    <outputDirectory>${project.build.outputDirectory}</outputDirectory>
                </configuration>
            </execution>
        </executions>
    </plugin>
```
To generate your initial expected schema file, just run the plugin and follow the instructions.

### Documentation

Documentation and links:

  * [Introduction](https://github.com/permazen/permazen/wiki/Introduction)
  * [Getting Started](https://github.com/permazen/permazen/wiki/GettingStarted)
  * [FAQ](https://github.com/permazen/permazen/wiki/FAQ)
  * [API Javadocs](https://permazen.github.io/permazen/site/apidocs/io/permazen/Permazen.html)
  * Bullet-point [JPA Comparison](https://github.com/permazen/permazen/wiki/JPA_Comparison)
  * [Key/value pair database layout](https://github.com/permazen/permazen/blob/master/LAYOUT.txt)
  * [Permazen Users](https://groups.google.com/forum/#!forum/permazen-users) discussion group
  * Auto-generated [Maven Site](https://permazen.github.io/permazen/site/)
