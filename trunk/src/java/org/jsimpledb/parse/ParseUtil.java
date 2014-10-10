
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.NoSuchElementException;

import org.jsimpledb.JClass;
import org.jsimpledb.JField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.parse.util.PrefixPredicate;
import org.jsimpledb.parse.util.StripPrefixFunction;

/**
 * Parsing utility routines.
 */
public final class ParseUtil {

    private ParseUtil() {
    }

    /**
     * Truncate a string with ellipsis if necessary.
     */
    public static String truncate(String string, int len) {
        if (len < 4)
            throw new IllegalArgumentException("len = " + len + " < 4");
        if (string.length() <= len)
            return string;
        return string.substring(0, len - 3) + "...";
    }

    /**
     * Generate completions based on a set of possibilities and the provided input prefix.
     */
    public static Iterable<String> complete(Iterable<String> choices, String prefix) {
        return Iterables.transform(Iterables.filter(choices, new PrefixPredicate(prefix)), new StripPrefixFunction(prefix));
    }

    /**
     * Locate the {@link JField} with the given name in the specified object.
     *
     * @param session current session
     * @param id object ID
     * @param name field name
     * @throws IllegalArgumentException if object does not exist
     * @throws IllegalArgumentException if object's type does not exist in schema
     * @throws IllegalArgumentException if field is not found
     * @throws IllegalArgumentException if any parameter is null
     */
    public static JField resolveJField(ParseSession session, ObjId id, String name) {

        // Sanity check
        if (session == null)
            throw new IllegalArgumentException("null session");
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (name == null)
            throw new IllegalArgumentException("null name");
        if (!session.hasJSimpleDB())
            throw new IllegalArgumentException("this session has no JSimpleDB");

        // Get object type
        final ObjInfo info = ObjInfo.getObjInfo(session, id);
        if (info == null)
            throw new IllegalArgumentException("error accessing field `" + name + "': object " + id + " does not exist");
        final ObjType objType = info.getObjType();

        // Find JClass
        final JClass<?> jclass;
        try {
            jclass = session.getJSimpleDB().getJClass(objType.getStorageId());
        } catch (UnknownTypeException e) {
            throw new IllegalArgumentException("error accessing field `" + name + "': " + e.getMessage(), e);
        }

        // Find JField
        final JField jfield = jclass.getJFieldsByName().get(name);
        if (jfield == null)
            throw new IllegalArgumentException("error accessing field `" + name + "': there is no such field in " + objType);
        return jfield;
    }

    /**
     * Locate the field with the given name in the specified object.
     *
     * @param session current session
     * @param id object ID
     * @param name field name
     * @throws IllegalArgumentException if object does not exist
     * @throws IllegalArgumentException if field is not found
     * @throws IllegalArgumentException if any parameter is null
     */
    public static Field<?> resolveField(ParseSession session, ObjId id, String name) {

        // Sanity check
        if (session == null)
            throw new IllegalArgumentException("null session");
        if (id == null)
            throw new IllegalArgumentException("null id");
        if (name == null)
            throw new IllegalArgumentException("null name");

        // Get object type
        final ObjInfo info = ObjInfo.getObjInfo(session, id);
        if (info == null)
            throw new IllegalArgumentException("error accessing field `" + name + "': object " + id + " does not exist");
        return ParseUtil.resolveField(session, info.getObjType(), name);
    }

    /**
     * Locate the field with the given name in the specified object type.
     *
     * @param session current session
     * @param objType object type
     * @param name field name
     * @throws IllegalArgumentException if field is not found
     * @throws IllegalArgumentException if any parameter is null
     */
    public static Field<?> resolveField(ParseSession session, ObjType objType, final String name) {

        // Sanity check
        if (session == null)
            throw new IllegalArgumentException("null session");
        if (objType == null)
            throw new IllegalArgumentException("null objType");
        if (name == null)
            throw new IllegalArgumentException("null name");

        // Find the field
        try {
            return Iterables.find(objType.getFields().values(), new Predicate<Field<?>>() {
                @Override
                public boolean apply(Field<?> field) {
                    return field.getName().equals(name);
                }
              });
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("error accessing field `" + name + "': there is no such field in " + objType);
        }
    }
}

