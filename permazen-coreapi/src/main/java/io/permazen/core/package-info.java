
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

/**
 * Core classes for the Permazen Java persistence layer.
 *
 * <p>
 * The classes in this package implement the JSimpleDB core API. The core API layer can function
 * entirely independently from the annotation-based {@link io.permazen.JSimpleDB} Java object model layer;
 * the latter is implemented on top of these classes. In turn, the core API layer is implemented on top
 * of the {@link io.permazen.kv key/value store} layer.
 *
 * @see io.permazen
 * @see io.permazen.kv
 * @see <a href="https://github.com/permazen/permazen/">The Permazen Project</a>
 */
package io.permazen.core;
