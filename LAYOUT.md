# Permazen Key/Value Database Structure

Permazen builds a database for persisting Java objects on top of a transactional `byte[]` array key/value store, where the keys are sorted lexicographically. Permazen encodes meta-data (including schema information), object state, and indexes into key/value pairs as described in this document.

Note: The `jsck` CLI command performs a consistency check of a Permazen key/value database to verify it is consistent with this document.

## Storage ID's

Storage ID's are unsigned 32-bit values that identify a range of related keys. Encoded storage ID's are used as prefixes for keys corresponding to specific object types, indexes, meta-data, etc.

To generate a `byte[]` array key prefix from a storage ID, the ID is encoded by [`UnsignedIntEncoder`](http://permazen.github.io/permazen/site/apidocs/io/permazen/util/UnsignedIntEncoder.html). Depending on the ID's value, the resulting `byte[]` array will be from 1 to 5 bytes. Typically, i.e., when using the [`DefaultStorageIdGenerator`](http://permazen.github.io/permazen/site/apidocs/io/permazen/DefaultStorageIdGenerator.html), it will be 3 bytes.

Storage ID's must be greater than zero.

## Object ID's

Objects are uniquely identified by their Object ID. An object ID is a 64-bit value consisting of the concatenation of:

* The storage ID corresponding to the object's type
* The object's unique random instance ID

An object's instance ID is a randomly chosen value.

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Storage ID ┃         Instance ID         ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

To create a new object, Permazen generates a new random instance ID and verifies that no matching object already exits, which almost always be the case unless the database is starting to get full for that object type.

The storage ID determines the maximum number of instances of an object type that may exist in the database. This will range from 2<sup>56</sup> (for storage ID's in the range zero to 250) down to 2<sup>40</sup> when using [`DefaultStorageIdGenerator`](http://permazen.github.io/permazen/site/apidocs/io/permazen/DefaultStorageIdGenerator.html).

Therefore, applications that expect to create a billion or more objects should configure custom single-byte storage ID's (see [`@PermazenType.storageId()`](http://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/PermazenType.html#storageId()), [`@JField.storageId()`](http://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/JField.html#storageId()), and [`@JCompositeIndex.storageId()`](http://permazen.github.io/permazen/site/apidocs/io/permazen/annotation/JCompositeIndex.html#storageId())) instead of relying on [`DefaultStorageIdGenerator`](http://permazen.github.io/permazen/site/apidocs/io/permazen/DefaultStorageIdGenerator.html).

## Database Key Ranges

The key/value pairs in a Permazen database are categorized (by prefix) below.

Unless specified otherwise, integral values are encoded into `byte[]`'s via [`UnsignedIntEncoder`](http://permazen.github.io/permazen/site/apidocs/io/permazen/util/UnsignedIntEncoder.html)

### The Meta-Data Range

The meta-data area spans all keys with first byte `0x00`.

* Keys with first byte `0x00` and second byte `0x00` through `0xfe` (inclusive) are reserved by Permazen and must not be modified.
* Keys with first byte `0x00` and second byte `0xff` are ignored by Permazen and may be used for custom user applications.

The meta-data contains the following information:

**Magic Cookie and Database Format Version**

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ 0x00 ┃ 0x00 ┃ 'Permazen' ┃ -> ┃  Database Format Version  ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

**Recorded Schemas**

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ 0x00 ┃ 0x01 ┃  Schema Version  ┃ -> ┃  Compressed Schema XML  ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━━━━━━━━━━┛

**Object Version Index**

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃ 0x00 ┃ 0x80 ┃  Schema Version  ┃  Object ID  ┃ -> ┃ (Empty) ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**User meta-data area**

    ┏━━━━━━┳━━━━━━┳━━━━-
    ┃ 0x00 ┃ 0xff ┃ ...
    ┗━━━━━━┻━━━━━━┻━━━━-

### Object Ranges

The keys associated with a specific object are prefixed with that object's Object ID.

Under the Object ID itself is meta-data related to the object:

    ┏━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┓
    ┃  Object ID  ┃ -> ┃  Object's Schema Version  ┃ Flags ┃
    ┗━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━┛

The 'Flags' field is a single byte. The only flag currently defined is the "Delete Notified" flag (`0x01`) which is used to track [`DeleteListener`](https://permazen.github.io/permazen/site/apidocs/io/permazen/core/DeleteListener.html) notifications.

#### Simple and Counter Fields

The value of a simple field is stored under the concatenation of the object ID and the field's storage ID:

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━┓
    ┃  Object ID ┃   Field ID   ┃ -> ┃  Field Value  ┃  (non-default only)
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━┛

Default values for simple fields (false, null, zero, etc.) are not stored.

#### Complex Fields

**Set Field Element**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Object ID ┃    Field ID   ┃    Element    ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**List Field Element**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━┓
    ┃  Object ID ┃    Field ID   ┃  List Index   ┃ -> ┃    Element    ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━┛

**Map Field Entry**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━┓
    ┃  Object ID ┃    Field ID   ┃    Map Key    ┃ -> ┃   Map Value   ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━┛

### Index Ranges

When a field is indexed, a secondary range of keys is created that maps the field's values back to the containing objects. These index ranges are described below.

#### Simple Indexes

Simple indexes map a single field's value back to the objects containing that value in that field.

**Simple Field Index**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Field ID  ┃     Value     ┃   Object ID   ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**Set Element Field Index**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Field ID  ┃    Element    ┃   Object ID   ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**List Element Field Index**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Field ID  ┃    Element    ┃   Object ID   ┃  List Index  ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**Map Key Field Index**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Field ID  ┃    Map Key    ┃   Object ID   ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**Map Value Field Index**

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Field ID  ┃   Map Value   ┃   Object ID   ┃    Map Key   ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

#### Composite Indexes

Composite indexes map a multiple field values back to the objects containing those values in those fields.

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━-┳...┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Index ID  ┃   Value 1   ┃...┃   Value N   ┃   Object ID  ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━┻...┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

