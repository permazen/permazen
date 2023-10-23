# Permazen Key/Value Database Structure

This file describes how objects, indexes, and meta-data are mapped into the key/value store.

## Storage ID's

Storage ID's are unsigned 32-bit values. They are encoded into `byte[]` arrays by the class
[`UnsignedIntEncoder`](http://permazen.github.io/permazen/site/apidocs/io/permazen/util/UnsignedIntEncoder.html), which means they can have variable length from 1 to 5 bytes (but typically 3).

Encoded storage ID's are used as prefixes for keys corresponding to specific object types, indexes, etc.

## Object ID's

Objects are uniquely identified by their Object ID. An object ID is a 64-bit value consisting of the concatenation of:

* The storage ID corresponding to the object's type
* The object's unique instance ID

An object's instance ID is a randomly chosen value.

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ Storage ID ┃         Instance ID         ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

## Database Layout

The key/value pairs in a Permazen database are categorized below.

Unless specified otherwise, integral values are encoded into `byte[]`'s via [`UnsignedIntEncoder`](http://permazen.github.io/permazen/site/apidocs/io/permazen/util/UnsignedIntEncoder.html)

The `jsck` CLI command consistency-checks a key/value database against the layout described here.

### Meta-Data

* The meta-data area spans all keys with first byte `0x00`
* Keys with first byte `0x00` and second byte `0x00` through `0xfe` (inclusive) are reserved by Permazen and must not be modified.
* Keys with first byte `0x00` and second byte `0xff` are ignored by Permazen and may be used for custom user applications.

**Magic Cookie and Database Format Version**

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━━━━━┓
    ┃ 0x00 ┃ 0x00 ┃ 'Permazen' ┃ -> ┃   Format Version   ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━━━━━┛

**Recorded Schemas**

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━━━━━━━━┓
    ┃ 0x00 ┃ 0x01 ┃  Version Number  ┃ -> ┃   Compressed Schema   ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━━━━━━━━┛

**Object Version Index**

    ┏━━━━━━┳━━━━━━┳━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃ 0x00 ┃ 0x80 ┃  Version Number  ┃   Object ID   ┃ -> ┃ (Empty) ┃
    ┗━━━━━━┻━━━━━━┻━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

**User meta-data area**

    ┏━━━━━━┳━━━━━━┳━━━━-
    ┃ 0x00 ┃ 0xff ┃ ...
    ┗━━━━━━┻━━━━━━┻━━━━-

### Objects

The keys associated with a specific object are all prefixed with the Object ID.

Under the Object ID itself is meta-data related to the object:

    ┏━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━━┳━━━━━━━┓
    ┃  Object ID ┃ -> ┃ Object Version ┃ Flags ┃
    ┗━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━━┻━━━━━━━┛

The 'Flags' field is a single byte. The only flag currently defined is the "Delete Notified" flag (`0x01`) which is used to track [`DeleteListener`](https://permazen.github.io/permazen/site/apidocs/io/permazen/core/DeleteListener.html) notifications.

### Simple and Counter Fields

The value of a simple field is stored under the concatenation of the object ID and the field's storage ID:

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓    ┏━━━━━━━━━━━━━━━┓
    ┃  Object ID ┃    Field ID   ┃ -> ┃     Value     ┃  (non-default only)
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━━━┛    ┗━━━━━━━━━━━━━━━┛

Note that default values of simple fields are _not_ stored explicitly.

### Complex Fields

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

### Simple Indexes

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

### Composite Indexes

    ┏━━━━━━━━━━━━┳━━━━━━━━━━━━-┳...┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓    ┏━━━━━━━━━┓
    ┃  Index ID  ┃   Value 1   ┃...┃   Value N   ┃   Object ID  ┃ -> ┃ (Empty) ┃
    ┗━━━━━━━━━━━━┻━━━━━━━━━━━━━┻...┻━━━━━━━━━━━━━┻━━━━━━━━━━━━━━┛    ┗━━━━━━━━━┛

