
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

/**
 * Represents a {@link JObject} for which no Java model type is defined in the current schema.
 *
 * <p>
 * Instances of this class are used to represent objects with a type that is defined in some older schema version
 * but not in the current schema version. This situation can occur when a new schema is added that "drops" a previously
 * defined Java model type, yet for which objects of that type still exist in the database. If encountered, these
 * objects will be represented by instances of this class. All object fields are still accessible, but they must be
 * accessed directly via the {@link JTransaction} field access methods.
 * </p>
 */
public abstract class UntypedJObject implements JObject {
}

