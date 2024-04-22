
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownTypeException;
import io.permazen.schema.SchemaId;
import io.permazen.util.ParseContext;

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
    public ObjType parse(Session session, String text) {
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(text != null, "null text");
        final ParserAction parserAction = new ParserAction(text);
        try {
            session.performSessionAction(parserAction);
        } catch (InterruptedException e) {
            throw new IllegalArgumentException("interrupted");
        }
        final ObjType result = parserAction.getResult();
        final Exception exception = parserAction.getException();
        if (exception != null || result == null) {
            throw exception instanceof IllegalArgumentException ?
              (IllegalArgumentException)exception : new IllegalArgumentException(exception);
        }
        return result;
    }

    private ObjType parseInTransaction(Session session, String text) {

        // Try to parse as an object type name with optional #schemaId suffix
        final ParseContext ctx = new ParseContext(text);
        final int startIndex = ctx.getIndex();
        final Matcher matcher;
        try {
            matcher = ctx.matchPrefix("(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)(#(Schema_[0-9a-f]{32}))?$");
        } catch (IllegalArgumentException e) {
            ctx.setIndex(startIndex);
            throw new IllegalArgumentException("invalid object type");
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
                throw new IllegalArgumentException(String.format("invalid object type schema \"%s\"", schemaId));
            }
        } else
            schema = tx.getSchema();

        // Find object type
        try {
            return schema.getObjType(typeName);
        } catch (UnknownTypeException e) {
            throw new IllegalArgumentException(String.format("unknown object type \"%s\"", typeName));
        }
    }

    private class ParserAction implements Session.RetryableTransactionalAction {

        private final String text;

        private ObjType result;
        private Exception exception;

        ParserAction(String text) {
            this.text = text;
        }

        @Override
        public void run(Session session) throws Exception {
            try {
                this.result = ObjTypeParser.this.parseInTransaction(session, this.text);
            } catch (IllegalArgumentException e) {
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
