
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.Set;
import java.util.regex.Matcher;

import org.jsimpledb.cli.Session;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaObject;
import org.jsimpledb.util.ParseContext;

/**
 * Parses an object type name.
 *
 * <p>
 * Syntax examples:
 * <ul>
 *  <li><code>100</code> - object type with schema ID 100</li>
 *  <li><code>Person</code> - `Person' object type defined in the current schema version</li>
 *  <li><code>Person#12</code> - `Person' object type defined in schema version 12</li>
 * </p>
 */
public class ObjTypeParser implements Parser<ObjType> {

    @Override
    public ObjType parse(Session session, ParseContext ctx, boolean complete) {

        // Try to parse as an integer
        final Transaction tx = session.getTransaction();
        final Database db = session.getDatabase();
        final int startIndex = ctx.getIndex();
        try {
            final int storageId = db.getFieldTypeRegistry().getFieldType(TypeToken.of(Integer.TYPE)).fromString(ctx);
            return tx.getSchemaVersion().getSchemaItem(storageId, ObjType.class);
        } catch (IllegalArgumentException e) {
            // ignore
        }
        ctx.setIndex(startIndex);

        // Try to parse as an object type name with optional #version suffix
        final Matcher matcher;
        try {
            matcher = ctx.matchPrefix("(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)(#([0-9]+))?");
        } catch (IllegalArgumentException e) {
            throw new ParseException(ctx, "invalid object type").addCompletions(session.getNameIndex().getSchemaObjectNames());
        }
        final String typeName = matcher.group(1);
        final String versionString = matcher.group(3);

        // Get name index
        final NameIndex nameIndex;
        if (versionString != null) {
            final int version;
            try {
                nameIndex = new NameIndex(tx.getSchema().getVersion(Integer.parseInt(versionString)).getSchemaModel());
            } catch (IllegalArgumentException e) {
                ctx.setIndex(startIndex);
                throw new ParseException(ctx, "invalid object type schema version `" + versionString + "'");
            }
        } else
            nameIndex = session.getNameIndex();

        // Find type by name
        final Set<SchemaObject> schemaObjects = nameIndex.getSchemaObjects(typeName);
        switch (schemaObjects.size()) {
        case 0:
            throw new ParseException(ctx, "unknown object type `" + typeName + "'")
               .addCompletions(ParseUtil.complete(nameIndex.getSchemaObjectNames(), typeName));
        case 1:
            return tx.getSchemaVersion().getSchemaItem(schemaObjects.iterator().next().getStorageId(), ObjType.class);
        default:
            throw new ParseException(ctx, "ambiguous object type `" + typeName + "': there are multiple matching object types"
              + " having storage IDs " + Lists.transform(Lists.newArrayList(schemaObjects),
                new Function<SchemaObject, Integer>() {
                    @Override
                    public Integer apply(SchemaObject schemaObject) {
                        return schemaObject.getStorageId();
                    }
               }));
        }
    }
}

