
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * Core classes for the JSimpleDB Java persistence layer.
 *
 * <p>
 * The classes in this package implement the JSimpleDB core API. The core API layer can function
 * entirely independently from the annotation-based {@link org.jsimpledb.JSimpleDB} Java object model layer;
 * the latter is implemented on top of these classes. In turn, the core API layer is implemented on top
 * of the {@link org.jsimpledb.kv key/value store} layer.
 *
 * @see org.jsimpledb
 * @see org.jsimpledb.kv
 * @see <a href="https://github.com/archiecobbs/jsimpledb/">The JSimpleDB Project</a>
 */
package org.jsimpledb.core;
