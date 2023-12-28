
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import io.permazen.cli.Session;
import io.permazen.core.CompositeIndex;
import io.permazen.core.ObjType;
import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

import java.util.SortedMap;
import java.util.regex.Matcher;

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
    public CompositeIndex parse(final Session session, final ParseContext ctx, final boolean complete) {

        // Get object type
        final ObjType objType = new ObjTypeParser().parse(session, ctx, complete);

        // Get composite index name and resolve index
        ctx.skipWhitespace();
        if (!ctx.tryLiteral("."))
            throw new ParseException(ctx, "expected composite index name").addCompletion(".");
        ctx.skipWhitespace();
        final SortedMap<String, CompositeIndex> indexMap = objType.getCompositeIndexes();
        final Matcher nameMatcher = ctx.tryPattern("\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*");
        if (nameMatcher == null)
            throw new ParseException(ctx, "expected composite index name").addCompletions(indexMap.keySet());
        final String indexName = nameMatcher.group();
        final CompositeIndex index = indexMap.get(indexName);
        if (index == null) {
            throw new ParseException(ctx, "unknown composite index \"" + indexName + "\" on " + objType)
              .addCompletions(ParseUtil.complete(indexMap.keySet(), indexName));
        }

        // Done
        return index;
    }
}
