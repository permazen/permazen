
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.TypeNotInSchemaException;

/**
 * Represents a {@link PermazenObject} for which no Java model type is defined in the instance's associated schema.
 *
 * <p>
 * Instances of this class are used to represent objects with a type that is defined in some other database schema
 * but not in the current one. This situation can occur when a new schema drops a previously defined Java model type
 * when there are objects still existing in the database. If encountered, such objects are represented by instances of
 * this class.
 *
 * <p>
 * These objects are still fully accessible, but they must be accessed via introspection using the {@link PermazenTransaction}
 * field access methods, with the {@code migrateSchema} parameter set to false (to prevent a {@link TypeNotInSchemaException}).
 *
 * <p>
 * For example, suppose a schema update removes the {@code Account} class and replaces fields referencing {@code Account}
 * objects with a simple {@link String} account ID field. Then the corresponding schema migration might look like this:
 * <pre>
 *      &#64;OnSchemaChange
 *      private void applySchemaChanges(Map&lt;String, Object&gt; oldValues) {
 *          if (oldValues.containsKey("account")) {                         // was replaced with "accountId"
 *              final PermazenObject acct = (PermazenObject)oldValues.get("account");     // acct has type UntypedPermazenObject
 *              final PermazenTransaction ptx = this.getTransaction();
 *              final String acctId = (String)ptx.readSimpleField(acct.getObjId(), "accountId", false);
 *              this.setAccountId(acctId);
 *          }
 *          // ...etc
 *      }
 * </pre>
 */
public abstract class UntypedPermazenObject implements PermazenObject {
}
