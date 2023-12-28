
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import io.permazen.cli.Session;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownTypeException;
import io.permazen.schema.SchemaId;
import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

import java.util.Optional;
import java.util.regex.Matcher;

/**
 * Parses an object type name.
 *
 * <p>
 * Syntax examples:
 * <ul>
 *  <li><code>Person</code> - `Person' object type in the current schema version</li>
 *  <li><code>Person#Schema_d462f3e631781b00ef812561115c48f6</code> - `Person' object type in the specified schema version</li>
 * </ul>
 */
public class ObjTypeParser implements Parser<ObjType> {

    @Override
    public ObjType parse(Session session, final ParseContext ctx, final boolean complete) {
        final ParserAction parserAction = new ParserAction(ctx, complete);
        try {
            session.performSessionAction(parserAction);
        } catch (InterruptedException e) {
            throw new ParseException(ctx, "interrupted");
        }
        final ObjType result = parserAction.getResult();
        final Exception exception = parserAction.getException();
        if (exception != null || result == null)
            throw exception instanceof ParseException ? (ParseException)exception : new ParseException(ctx, exception);
        return result;
    }

    private ObjType parseInTransaction(Session session, final ParseContext ctx, final boolean complete) {

        // Try to parse as an object type name with optional #schemaId suffix
        final int startIndex = ctx.getIndex();
        final Matcher matcher;
        try {
            matcher = ctx.matchPrefix("(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)(#(Schema_[0-9a-f]{32}))?$");
        } catch (IllegalArgumentException e) {
            ctx.setIndex(startIndex);
            throw new ParseException(ctx, "invalid object type")
              .addCompletions(session.getSchemaModel().getSchemaObjectTypes().keySet());
        }
        final String typeName = matcher.group(1);
        final SchemaId schemaId = Optional.ofNullable(matcher.group(3)).map(SchemaId::new).orElse(null);

        // Get specified schema version and corresponding name index
        final Transaction tx = session.getTransaction();
        final Schema schema;
        if (schemaId != null) {
            try {
                schema = tx.getSchemaBundle().getSchema(schemaId);
            } catch (IllegalArgumentException e) {
                ctx.setIndex(startIndex);
                throw new ParseException(ctx, String.format("invalid object type schema \"%s\"", schemaId));
            }
        } else
            schema = tx.getSchema();

        // Find object type
        try {
            return schema.getObjType(typeName);
        } catch (UnknownTypeException e) {
            throw new ParseException(ctx, String.format("unknown object type \"%s\"", typeName))
               .addCompletions(ParseUtil.complete(schema.getObjTypes().keySet(), typeName));
        }
    }

    private class ParserAction implements Session.Action, Session.RetryableAction {

        private final ParseContext ctx;
        private final boolean complete;

        private ObjType result;
        private Exception exception;

        ParserAction(ParseContext ctx, boolean complete) {
            this.ctx = ctx;
            this.complete = complete;
        }

        @Override
        public void run(Session session) throws Exception {
            try {
                this.result = ObjTypeParser.this.parseInTransaction(session, this.ctx, this.complete);
            } catch (ParseException e) {
                this.exception = e;
            }
        }

        public ObjType getResult() {
            return this.result;
        }

        public Exception getException() {
            return this.exception;
        }
    }
}
