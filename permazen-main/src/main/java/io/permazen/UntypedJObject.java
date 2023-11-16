
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.TypeNotInSchemaVersionException;

/**
 * Represents a {@link JObject} for which no Java model type is defined in the instance's associated schema version.
 *
 * <p>
 * Instances of this class are used to represent objects with a type that is defined in some database schema version
 * other than the current one. This situation can occur when a new schema drops a previously defined Java model type
 * of which type objects still exist in the database. If encountered, such objects are represented by instances of
 * this class.
 *
 * <p>
 * These objects are still fully accessible, but they must be accessed via introspection using the
 * {@link JTransaction} field access methods, with the {@code upgradeVersion} parameter set to false
 * (to prevent a {@link TypeNotInSchemaVersionException}).
 *
 * <p>
 * For example, suppose a schema update replaces a field referencing {@code Account} with a simple {@link String}
 * field containing the account ID. Then the corresponding schema migration might look like this:
 * <pre>
 *      &#64;OnVersionChange
 *      private void applySchemaChanges(Map&lt;String, Object&gt; oldValues) {
 *          if (oldValues.containsKey("account")) {                         // was replaced with "accountId"
 *              final JObject acct = (JObject)oldValues.get("account");     // acct has type UntypedJObject
 *              final JTransaction jtx = this.getTransaction();
 *              final String acctId = (String)jtx.readSimpleField(acct.getObjId(), 12345, false);
 *              this.setAccountId(acctId);
 *          }
 *          // ...etc
 *      }
 * </pre>
 */
public abstract class UntypedJObject implements JObject {
}
