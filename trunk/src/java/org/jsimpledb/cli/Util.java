
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;

import org.dellroad.stuff.string.ParseContext;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaObject;
import org.jsimpledb.util.NavigableSets;

public final class Util {

    private Util() {
    }

    /**
     * Get object meta-data.
     *
     * @return object info, or null if object doesn't exist
     */
    public static ObjInfo getObjInfo(Transaction tx, ObjId id) throws IOException {
        try {
            return new ObjInfo(tx, id);
        } catch (DeletedObjectException e) {
            return null;
        }
    }

    /**
     * Parse an object type name.
     */
    public static SchemaObject parseSchemaObject(Session session, ParseContext ctx, String usage) throws ParseException {
        final NameIndex nameIndex = session.getNameIndex();
        SortedSet<String> typeNames = nameIndex != null ? nameIndex.getSchemaObjectNames() : new TreeSet<String>();
        String typeName = null;
        try {
            typeName = ctx.matchPrefix("([^\\s]+)\\s*$").group(1);
            if (!typeNames.contains(typeName))
                throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
            if (typeName != null)
                typeNames = Sets.filter(typeNames, new PrefixPredicate(typeName));
            throw new ParseException(ctx, "Usage: " + usage).addCompletions(typeNames);
        }
        return nameIndex.getSchemaObject(typeName);
    }

    /**
     * Parse an object ID.
     */
    public static ObjId parseObjId(Session session, ParseContext ctx, String usage) throws ParseException {

        // Get parameter
        final String param = ctx.getInput().trim();

        // Attempt to parse it whole
        try {
            return new ObjId(param);
        } catch (IllegalArgumentException e) {
            // parse failed
        }

        // Param must look like a hex ID
        if (!param.matches("[0-9A-Fa-f]{0,16}"))
            throw new ParseException(ctx, usage);

        // Get corresponding min & max ObjId (both inclusive)
        final char[] paramChars = param.toCharArray();
        final char[] idChars = new char[16];
        System.arraycopy(paramChars, 0, idChars, 0, paramChars.length);
        Arrays.fill(idChars, paramChars.length, idChars.length, '0');
        final String minString = new String(idChars);
        ObjId id;
        try {
            id = new ObjId(minString);
        } catch (IllegalArgumentException e) {
            if (!minString.startsWith("00"))
                throw new ParseException(ctx, usage);
            id = null;
        }
        final ObjId min = id;
        Arrays.fill(idChars, paramChars.length, idChars.length, 'f');
        try {
            id = new ObjId(new String(idChars));
        } catch (IllegalArgumentException e) {
            id = null;
        }
        final ObjId max = id;

        // Find object IDs in the range
        final ArrayList<String> completions = new ArrayList<>();
        session.perform(new TransactionAction() {
            @Override
            public void run(Session session) throws Exception {
                final Transaction tx = session.getTransaction();
                final TreeSet<Integer> storageIds = new TreeSet<>();
                final ArrayList<NavigableSet<ObjId>> idSets = new ArrayList<>();
                for (SchemaVersion schemaVersion : tx.getSchema().getSchemaVersions().values()) {
                    for (SchemaItem schemaItem : schemaVersion.getSchemaItemMap().values()) {
                        if (!(schemaItem instanceof ObjType))
                            continue;
                        final int storageId = schemaItem.getStorageId();
                        if ((min != null && storageId < min.getStorageId()) || (max != null && storageId > max.getStorageId()))
                            continue;
                        NavigableSet<ObjId> idSet = tx.getAll(storageId);
                        if (min != null) {
                            try {
                                idSet = idSet.tailSet(min, true);
                            } catch (IllegalArgumentException e) {
                                // ignore
                            }
                        }
                        if (max != null) {
                            try {
                                idSet = idSet.headSet(max, true);
                            } catch (IllegalArgumentException e) {
                                // ignore
                            }
                        }
                        idSets.add(idSet);
                    }
                }
                int count = 0;
                for (ObjId id : NavigableSets.union(idSets)) {
                    completions.add(id.toString());
                    count++;
                    if (count >= session.getLineLimit() + 1)
                        break;
                }
            }
        });

        // Throw exception with completions
        throw new ParseException(ctx, usage).addCompletions(completions);
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

