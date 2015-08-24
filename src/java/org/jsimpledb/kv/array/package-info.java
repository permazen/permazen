
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * A simple {@link org.jsimpledb.kv.KVStore} implementation based on a sorted array of key/value pairs.
 *
 * <p>
 * Instances are optimized for relatively few writes and have minimal memory overhead.
 *
 * <p>
 * Key and value data must each not exceed 2GB.
 */
package org.jsimpledb.kv.array;
