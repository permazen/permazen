
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.regex.Matcher;

import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.Schema;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.NavigableSets;
import org.jsimpledb.util.ParseContext;

/**
 * Parses object IDs.
 */
public class ObjIdParser implements Parser<ObjId> {

    public static final int MAX_COMPLETE_OBJECTS = 100;

    @Override
    public ObjId parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Get parameter
        final Matcher matcher = ctx.tryPattern("([0-9A-Fa-f]{0,16})");
        if (matcher == null)
            throw new ParseException(ctx, "invalid object ID");
        final String param = matcher.group(1);

        // Attempt to parse id
        try {
            return new ObjId(param);
        } catch (IllegalArgumentException e) {              // must be a partial ID
            if (!ctx.isEOF() || !complete)
                throw new ParseException(ctx, "invalid object ID");
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
                throw new ParseException(ctx, "invalid object ID");
            min0 = null;
        }
        Arrays.fill(idChars, paramChars.length, idChars.length, 'f');
        ObjId max0;
        try {
            max0 = new ObjId(new String(idChars));
        } catch (IllegalArgumentException e) {
            max0 = null;
        }

        // TODO: if multiple storage ID's match, complete only through the storage ID

        // Find object IDs in the range
        final ArrayList<String> completions = new ArrayList<>();
        final ObjId min = min0;
        final ObjId max = max0;
        session.performParseSessionAction(new ParseSession.RetryableAction() {
            @Override
            public void run(ParseSession session) throws Exception {
                final Transaction tx = session.getTransaction();
                final ArrayList<NavigableSet<ObjId>> idSets = new ArrayList<>();
                completions.clear();
                for (Schema schema : tx.getSchemas().getVersions().values()) {
                    for (ObjType objType : schema.getObjTypes().values()) {
                        final int storageId = objType.getStorageId();
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
                if (!idSets.isEmpty()) {
                    for (ObjId id : Iterables.limit(NavigableSets.union(idSets), MAX_COMPLETE_OBJECTS))
                        completions.add(id.toString());
                }
            }
        });

        // Throw exception with completions
        throw new ParseException(ctx, "invalid object ID").addCompletions(ParseUtil.complete(completions, param));
    }
}

