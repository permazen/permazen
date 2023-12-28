# Permazen Key/Value Database Structure

Permazen builds a database for persisting Java objects on top of a transactional `byte[]` array key/value store, where the keys are sorted lexicographically. Permazen encodes meta-data (including schema information), object state, and indexes into key/value pairs as described in this document.

Note: The `jsck` CLI command performs a consistency check of a Permazen key/value database to verify it is consistent with this document.

Unless specified otherwise:

* Unsigned integral values (e.g., storage ID's) are encoded into `byte[]`'s via [`UnsignedIntEncoder`](http://permazen.github.io/permazen/site/apidocs/io/permazen/util/UnsignedIntEncoder.html)
* Strings in `'single quotes'` are encoded into `byte[]`'s via [`StringEncoding`](http://permazen.github.io/permazen/site/apidocs/io/permazen/encoding/StringEncoding.html)

## Storage ID's

Storage ID's are positive 32-bit values that identify a range of related keys. Encoded storage ID's are used as prefixes for keys corresponding to specific object types, indexes, meta-data, etc.

To generate a `byte[]` array key prefix from a storage ID, the ID is encoded by [`UnsignedIntEncoder`](http://permazen.github.io/permazen/site/apidocs/io/permazen/util/UnsignedIntEncoder.html). Depending on the ID's value, the resulting `byte[]` array will be from 1 to 5 bytes (values from 1 to 250 require one byte, values from 251 to 506 require two bytes, values from 507 to 65786 require three bytes, etc.).

By default, storage ID's are assigned automatically starting from 1, so unless the schema is very complicated, they will all fit in one byte.

## Object ID's

Objects are uniquely identified by their Object ID. An object ID is a 64-bit value consisting of the concatenation of:

* The storage ID corresponding to the object's type
* Random bits to make the object ID unique

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Storage ID ┃     Random Bits      ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━┛

To create a new object, Permazen generates a new random ID and verifies that no such object already exits, which highly likely unless the database is getting full for that object type. In the common case of one byte storage ID's, that would imply quadrillions of objects.

All of the data associated with a specific object is stored under keys prefixed by the object's Object ID (see "Object Ranges" below).

The object type storage ID determines the maximum number of instances of an object type that may exist in the database, which is 2<sup>64 - 8L</sup> where L is the number of bytes required to encode the storage ID, so typically 2<sup>56</sup>.

## Database Key Ranges

The key/value pairs in a Permazen database are categorized (by prefix) below.

### The Meta-Data Range

The meta-data area spans all keys with first byte `0x00`.

* Keys with first byte `0x00` and second byte `0x00` through `0xfe` (inclusive) are reserved by Permazen and must not be modified.
* Keys with first byte `0x00` and second byte `0xff` are ignored by Permazen and may be used for custom user applications.

The meta-data contains the following information:

**Magic Cookie and Database Format Version**

A Permazen database has a "magic cookie" key/value pair that also encodes a format version to allow for future changes:

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ 0x00 ┃ 0x00 ┃ 'Permazen' ┃ -> ┃  Database Format Version  ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

The current format version is [`Layout.CURRENT_FORMAT_VERSION`](https://permazen.github.io/permazen/site/apidocs/io/permazen/core/Layout.html#CURRENT_FORMAT_VERSION),

**Schema Table**

Schemas are recorded in an indexed list. The "schema index" is a positive integer encoded like a storage ID.

The list can have holes if unused schema(s) have been garbage collected. When a new schema is recorded, it always grabs the lowest available schema index.

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ 0x00 ┃ 0x01 ┃  Schema Index  ┃ -> ┃  Compressed Schema XML  ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━━━━━━━━━━┛

The schema index is stored under each object's key to indicate its schema version (see below).

**Storage ID Table**

All object types, fields, and composite indexes have an associated [`SchemaId`](http://permazen.github.io/permazen/site/apidocs/io/permazen/schema/SchemaId.html) which is a unique signature corresponding to how the item is encoded in the database, independent of the schema that it belongs to.

This means (for example) that objects from different schemas but having the same type name will be stored together, and that fields from different schemas with the same name and encoding will be indexed together. This is what makes it possible to return objects from other schema versions when querying by type or index.

`SchemaId`'s are hash values generated based on the item type, plus the following:

* For object types, their object type names (regardless of the fields they contain)
* For counter fields, their names
* For simple fields, their names and [`EncodingId`](http://permazen.github.io/permazen/site/apidocs/io/permazen/encoding/EncodingId.html)
* For complex fields, their names and the `SchemaId`'s of their sub-fields
* For composite indexes, the `SchemaId`'s of the fields in the index

Each `SchemaId` is assigned a unique storage ID. Storage ID's are used to build keys in the key/value database.

Storage ID's are allocated consecutively starting at one and stored in the Storage ID Table list:

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━┓
    ┃ 0x00 ┃ 0x02 ┃  Storage ID  ┃ -> ┃  SchemaId  ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━┛

When recording a new schema, any storage ID's not already assigned get the next available storage ID. When a schema is removed, unused storage IDs are garbage collected, so this table can also have holes.

Note: A schema itself also has a `SchemaId`, which is a hash over the entire schema. This hash is used to quickly determine if two schemas are identical.

**Object Schema Index**

This table indexes objects by their schema index.

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃ 0x00 ┃ 0x80 ┃  Schema Index  ┃  Object ID  ┃ -> ┃ (Empty) ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**User meta-data area**

This region is always ignored by Permazen.

    ┏━━━━━━┳━━━━━━┳━━━━-
    ┃ 0x00 ┃ 0xff ┃ ...
    ┗━━━━━━┻━━━━━━┻━━━━-

### Object Ranges

The keys associated with a specific object are prefixed with that object's Object ID.

Under the Object ID itself is meta-data related to the object:

    ┏━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━┳━━━━━━━┓
    ┃  Object ID  ┃ -> ┃  Schema Index  ┃ Flags ┃
    ┗━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━┻━━━━━━━┛

The 'Flags' field is a single byte. The only flag currently defined is the "Delete Notified" flag (`0x01`) which is used to track [`DeleteListener`](https://permazen.github.io/permazen/site/apidocs/io/permazen/core/DeleteListener.html) notifications. All other bits must be zero.

#### Simple and Counter Fields

The value of a simple field is stored under the concatenation of the object ID and the field's storage ID ("Field ID"):

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

When a field is indexed, a secondary range of keys is created that maps the field's values back to the objects having that value in the field. These index ranges are described below.

#### Simple Indexes

Simple indexes map a simple field's value back to the objects containing that value in that field.

For list element and map value sub-fields, the list index and map key (respectively) is also included in the index.

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

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━┳...┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Index ID  ┃   Value 1   ┃...┃   Value N   ┃   Object ID  ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━┻...┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

