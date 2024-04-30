
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * A simple {@link io.permazen.kv.KVStore} implementation based on a sorted array of key/value pairs.
 *
 * <p>
 * Instances are optimized for relatively few writes and have minimal memory overhead.
 *
 * <p>
 * Key and value data must not exceed 2GB (each separately).
 *
 * <p><b>File Format</b>
 *
 * <p>
 * There are three files: <b>index</b>, <b>keys</b>, and <b>values</b>.
 *
 * <p>
 * The <b>keys</b> file contains all of the database keys, concatenated and in order, encoding using a special compression
 * scheme (see below).
 *
 * <p>
 * The <b>values</b> file contains all of the database values, concatenated and in order, uncompressed.
 *
 * <p>
 * The <b>index</b> file contains zero or more index entries, one entry for each key/value pair, concatenated and in order.
 * An index entry is a pair of big endian 32-bit values, where the first value describes the offset of the corresponding key
 * in the <b>keys</b> file using a special encoding (see below), while the second describes the absolute offset of the
 * corresponding value in the <b>values</b> file.
 *
 * <p>
 * For index entries in a slot equal to zero mod 32, the first 32-bit value is the absolute offset of the key in the <b>keys</b>
 * file; this is called a "base key". For other index entries, the first 8 bits are how many bytes of the previous base key's
 * prefix match this key (from zero to 255), and the remaining 24 bits are the offset in bytes from the beginning of the previous
 * base key in the <b>keys</b> file of the start of this key's suffix. The end of the key in the <b>keys</b> file is implied by
 * where the next key starts (or end of file).
 *
 * <p>
 * For all index entries, the second 32-bit value is the absolute offset of the value in the values file.
 * The end of the value in the <b>value</b> file is implied by where the next value starts (or end of file).
 */
package io.permazen.kv.array;
