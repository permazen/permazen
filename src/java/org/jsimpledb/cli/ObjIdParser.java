
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.NavigableSets;
import org.jsimpledb.util.ParseContext;

/**
 * Parses object IDs.
 */
public class ObjIdParser implements Parser<ObjId> {

    @Override
    public ObjId parse(Session session, ParseContext ctx, boolean complete) {

        // Get parameter
        final Matcher matcher = ctx.tryPattern("([0-9A-Fa-f]{0,16})");
        if (matcher == null)
            throw new ParseException(ctx, "invalid object ID starting with `" + Util.truncate(ctx.getInput(), 16) + "'");
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
                throw new ParseException(ctx, "invalid object ID starting with `" + Util.truncate(ctx.getInput(), 16) + "'");
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
                        if ((min != null && storageId < min.getStorageId())
                          || (max != null && storageId > max.getStorageId()))
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
        throw new ParseException(ctx, "invalid object ID starting with `" + Util.truncate(ctx.getInput(), 16) + "'")
          .addCompletions(Util.complete(completions, param));
    }
}

