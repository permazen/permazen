
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaObject;
import org.jsimpledb.util.NavigableSets;
import org.jsimpledb.util.ParseContext;

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
    public static Iterable<String> complete(Iterable<String> choices, String prefix, boolean spacePrefix) {
        return Iterables.transform(
          Iterables.transform(
           Iterables.transform(Iterables.filter(choices, new PrefixPredicate(prefix)), new StripPrefixFunction(prefix)),
          new AddPrefixFunction(spacePrefix ? " " : "")),
        new AddSuffixFunction(" "));
    }

    /**
     * Parse an object type name.
     */
    public static int parseTypeName(Session session, ParseContext ctx, String param) {

        // Get name index
        final NameIndex nameIndex = session.getNameIndex();

        // Try to parse as an integer
        try {
            return Integer.parseInt(param);
        } catch (NumberFormatException e) {
            // ignore
        }

        // Try to parse as a name
        final Set<SchemaObject> schemaObjects = nameIndex.getSchemaObjects(param);
        switch (schemaObjects.size()) {
        case 0:
            throw new ParseException(ctx, "unknown object type `" + param + "'").addCompletions(
              Util.complete(nameIndex.getSchemaObjectNames(), param, false));
        case 1:
            return schemaObjects.iterator().next().getStorageId();
        default:
            throw new ParseException(ctx, "there are multiple object types named `" + param + "'; specify by storage ID");
        }

        // TODO: support "Person#3" with explicit version specified
    }

    /**
     * Parse an object ID.
     */
    public static ObjId parseObjId(Session session, ParseContext ctx, String usage) {

        // Get parameter
        final Matcher matcher = ctx.tryPattern("\\s*([0-9A-Fa-f]{0,16})(\\s.*)?$");
        if (matcher == null)
            throw new ParseException(ctx, "Usage: " + usage);
        final String param = matcher.group(1);

        // Attempt to parse id
        try {
            return new ObjId(param);
        } catch (IllegalArgumentException e) {
            // parse failed - must be a partial ID
        }

        // Get corresponding min & max ObjId (both inclusive)
        final char[] paramChars = param.toCharArray();
        final char[] idChars = new char[16];
        System.arraycopy(paramChars, 0, idChars, 0, paramChars.length);
        Arrays.fill(idChars, paramChars.length, idChars.length, '0');
        final String minString = new String(idChars);
        ObjId min0;
        try {
            min0 = new ObjId(minString);
        } catch (IllegalArgumentException e) {
            if (!minString.startsWith("00"))
                throw new ParseException(ctx, usage);
            min0 = null;
        }
        Arrays.fill(idChars, paramChars.length, idChars.length, 'f');
        ObjId max0;
        try {
            max0 = new ObjId(new String(idChars));
        } catch (IllegalArgumentException e) {
            max0 = null;
        }

        // Find object IDs in the range
        final ArrayList<String> completions = new ArrayList<>();
        final ObjId min = min0;
        final ObjId max = max0;
        session.perform(new Action() {
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
                    completions.add(id + " ");
                    count++;
                    if (count >= session.getLineLimit() + 1)
                        break;
                }
            }
        });

        // Throw exception with completions
        throw new ParseException(ctx, usage).addCompletions(Util.complete(completions, param, false));
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

