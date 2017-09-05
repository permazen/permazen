
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
 * <b>File Format</b>
 *
 * <p>
 * There are three files: index, keys, and values.
 *
 * <p>
 * The index file contains zero or more index entries, which are pairs of big endian 32-bit values where the first value
 * in a pair describes the offset of the corresponding key in the keys file, while the second describes the offset of the
 * corresponding value in the values file. Index entries are sorted by key.
 *
 * <p>
 * For index entries in a slot equal to zero mod 32, the 32-bit value is the absolute offset of the key, and this is called
 * a "base key"; otherwise, the first 8 bits are the length of the key's prefix matching the previous base key (from zero to 255),
 * while the remaining 24 bits are the offset from the beginning of the previous base key to the start of the suffix. In both
 * cases, the end of the key is the starting offset of the next key (or end of file).
 *
 * <p>
 * For all index entries, the second 32-bit value is the absolute offset of the value in the values file.
 * The end of the value is the starting offset of the next value (or end of file).
 */
package io.permazen.kv.array;
