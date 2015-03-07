
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse;

import com.google.common.reflect.TypeToken;

import java.util.regex.Matcher;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.Schema;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaObjectType;

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
    public ObjType parse(ParseSession session, ParseContext ctx, boolean complete) {

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
                throw new ParseException(ctx, "invalid object type schema version `" + versionString + "'");
            }
            nameIndex = new NameIndex(schema.getSchemaModel());
        } else {
            schema = tx.getSchema();
            nameIndex = session.getNameIndex();
        }

        // Find type by name
        final SchemaObjectType schemaObjectType = nameIndex.getSchemaObjectType(typeName);
        if (schemaObjectType == null) {
            throw new ParseException(ctx, "unknown object type `" + typeName + "'")
               .addCompletions(ParseUtil.complete(nameIndex.getSchemaObjectTypeNames(), typeName));
        }
        return schema.getObjType(schemaObjectType.getStorageId());
    }
}

