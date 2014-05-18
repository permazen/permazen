
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterables;

import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;

public final class Util {

    private Util() {
    }

    /**
     * Get object meta-data.
     *
     * @return object info, or null if object doesn't exist
     */
    public static ObjInfo getObjInfo(Session session, ObjId id) {
        try {
            return new ObjInfo(session.getTransaction(), id);
        } catch (DeletedObjectException e) {
            return null;
        }
    }

    /**
     * Generate completions based on a set of possibilities and the provided input prefix.
     */
    public static Iterable<String> complete(Iterable<String> choices, String prefix) {
        return Iterables.transform(
          Iterables.transform(Iterables.filter(choices, new PrefixPredicate(prefix)), new StripPrefixFunction(prefix)),
        new AddSuffixFunction(" "));
    }

// ObjInfo

    /**
     * Object meta-data.
     */
    public static class ObjInfo {

        private final ObjId id;
        private final SchemaVersion schemaVersion;
        private final ObjType type;

        public ObjInfo(Transaction tx, ObjId id) {
            this.id = id;
            this.schemaVersion = tx.getSchema().getVersion(tx.getSchemaVersion(id));
            this.type = this.schemaVersion.getSchemaItem(id.getStorageId(), ObjType.class);
        }

        public ObjId getObjId() {
            return this.id;
        }

        public SchemaVersion getSchemaVersion() {
            return this.schemaVersion;
        }

        public ObjType getObjType() {
            return this.type;
        }

        @Override
        public String toString() {
            return this.id + " type " + this.type.getName() + "#" + this.type.getStorageId()
              + " version " + this.schemaVersion.getVersionNumber();
        }
    }
}

