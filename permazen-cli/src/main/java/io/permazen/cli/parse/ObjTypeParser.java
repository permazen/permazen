
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import com.google.common.reflect.TypeToken;

import io.permazen.cli.Session;
import io.permazen.core.Database;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownTypeException;
import io.permazen.schema.NameIndex;
import io.permazen.schema.SchemaObjectType;
import io.permazen.util.ParseContext;
import io.permazen.util.ParseException;

import java.util.regex.Matcher;

/**
 * Parses an object type name.
 *
 * <p>
 * Syntax examples:
 * <ul>
 *  <li><code>100</code> - object type with schema ID 100</li>
 *  <li><code>Person</code> - `Person' object type defined in the current schema version</li>
 *  <li><code>Person#12</code> - `Person' object type defined in schema version 12</li>
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

        // Try to parse as an integer
        final Transaction tx = session.getTransaction();
        final Database db = session.getDatabase();
        final int startIndex = ctx.getIndex();
        try {
            final int storageId = db.getFieldTypeRegistry().getFieldType(TypeToken.of(Integer.TYPE)).fromParseableString(ctx);
            return tx.getSchema().getObjType(storageId);
        } catch (IllegalArgumentException | UnknownTypeException e) {
            // ignore
        }
        ctx.setIndex(startIndex);

        // Try to parse as an object type name with optional #version suffix
        final Matcher matcher;
        try {
            matcher = ctx.matchPrefix("(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)(#([0-9]+))?");
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid object type").addCompletions(session.getNameIndex().getSchemaObjectTypeNames());
        }
        final String typeName = matcher.group(1);
        final String versionString = matcher.group(3);

        // Get specified schema version and corresponding name index
        final Schema schema;
        final NameIndex nameIndex;
        if (versionString != null) {
            try {
                schema = tx.getSchemas().getVersion(Integer.parseInt(versionString));
            } catch (IllegalArgumentException e) {
                ctx.setIndex(startIndex);
                throw new ParseException(ctx, "invalid object type schema version \"" + versionString + "\"");
            }
            nameIndex = new NameIndex(schema.getSchemaModel());
        } else {
            schema = tx.getSchema();
            nameIndex = session.getNameIndex();
        }

        // Find type by name
        final SchemaObjectType schemaObjectType = nameIndex.getSchemaObjectType(typeName);
        if (schemaObjectType == null) {
            throw new ParseException(ctx, "unknown object type \"" + typeName + "\"")
               .addCompletions(ParseUtil.complete(nameIndex.getSchemaObjectTypeNames(), typeName));
        }
        return schema.getObjType(schemaObjectType.getStorageId());
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

