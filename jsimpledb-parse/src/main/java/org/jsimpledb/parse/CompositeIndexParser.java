
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse;

import java.util.SortedMap;
import java.util.regex.Matcher;

import org.jsimpledb.core.CompositeIndex;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.util.ParseContext;

/**
 * Parses a composite index.
 *
 * <p>
 * Syntax examples:
 * <ul>
 *  <li><code>Person.byLastFirst</code></li>
 *  <li><code>Person.mycompositeindex</code></li>
 * </ul>
 */
public class CompositeIndexParser implements Parser<CompositeIndex> {

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public CompositeIndex parse(final ParseSession session, final ParseContext ctx, final boolean complete) {

        // Get object type
        final int typeStart = ctx.getIndex();
        final ObjType objType = new ObjTypeParser().parse(session, ctx, complete);

        // Get composite index name and resolve index
        ctx.skipWhitespace();
        if (!ctx.tryLiteral("."))
            throw new ParseException(ctx, "expected composite index name").addCompletion(".");
        ctx.skipWhitespace();
        final SortedMap<String, CompositeIndex> indexMap = objType.getCompositeIndexesByName();
        final Matcher nameMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
        if (nameMatcher == null)
            throw new ParseException(ctx, "expected composite index name").addCompletions(indexMap.keySet());
        final String indexName = nameMatcher.group();
        final CompositeIndex index = indexMap.get(indexName);
        if (index == null) {
            throw new ParseException(ctx, "unknown composite index `" + indexName + "' on " + objType)
              .addCompletions(ParseUtil.complete(indexMap.keySet(), indexName));
        }

        // Done
        return index;
    }
}

