
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

/**
 * Represents a {@link JObject} for which no Java model type is defined in the instance's associated schema version.
 *
 * <p>
 * Instances of this class are used to represent objects with a type that is defined in some older schema version
 * but not in the current schema version. This situation can occur when a new schema is added that drops a previously
 * defined Java model type, yet for which objects of that type still exist in the database. If encountered, these
 * objects will be represented by instances of this class.
 *
 * <p>
 * All object fields are still accessible, but they must be accessed directly via introspection using the {@link JTransaction}
 * field access methods with {@code upgradeVersion} set to false (to prevent a
 * {@link io.permazen.core.TypeNotInSchemaVersionException}).
 */
public abstract class UntypedJObject implements JObject {
}

