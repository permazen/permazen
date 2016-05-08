
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.NoSuchElementException;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.JClass;
import org.jsimpledb.JField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.parse.util.PrefixPredicate;
import org.jsimpledb.parse.util.StripPrefixFunction;

/**
 * Parsing utility routines.
 */
public final class ParseUtil {

    /**
     * Regular expression that matches valid Java identifiers.
     */
    public static final String IDENT_PATTERN = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";

    private ParseUtil() {
    }

    /**
     * Generate completions based on a set of possibilities and the provided input prefix.
     *
     * @param choices possible choices
     * @param prefix prefix input so far
     * @return possible completions
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
     * @return the specified field
     * @throws IllegalArgumentException if object does not exist
     * @throws IllegalArgumentException if object's type does not exist in schema
     * @throws IllegalArgumentException if field is not found
     * @throws IllegalArgumentException if any parameter is null
     * @throws IllegalArgumentException if {@code state} is not in {@link org.jsimpledb.SessionMode#JSIMPLEDB}
     */
    public static JField resolveJField(ParseSession session, ObjId id, String name) {

        // Sanity check
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(session.getMode().hasJSimpleDB(), "session mode has no JSimpleDB");

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
     * @return the specified field
     * @throws IllegalArgumentException if object does not exist
     * @throws IllegalArgumentException if field is not found
     * @throws IllegalArgumentException if any parameter is null
     */
    public static Field<?> resolveField(ParseSession session, ObjId id, String name) {

        // Sanity check
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(name != null, "null name");

        // Get object type
        final ObjInfo info = ObjInfo.getObjInfo(session, id);
        if (info == null)
            throw new IllegalArgumentException("error accessing field `" + name + "': object " + id + " does not exist");
        final ObjType objType = info.getObjType();

        // Find the field
        try {
            return Iterables.find(objType.getFields().values(), new HasNamePredicate(name));
        } catch (NoSuchElementException e) {
            throw new IllegalArgumentException("error accessing field `" + name + "': there is no such field in " + objType);
        }
    }

    /**
     * Get the array class with the given non-array base type and dimensions.
     *
     * @param base base type
     * @param dims number of dimensions
     * @return the corresponding array type, or {@code base} if {@code dims} is zero
     * @throws IllegalArgumentException if {@code base} is null
     * @throws IllegalArgumentException if {@code base} is an array type
     * @throws IllegalArgumentException if {@code base} is {@code void} and {@code dims > 0}
     * @throws IllegalArgumentException if {@code dims} is not in the range 0..255
     */
    public static Class<?> getArrayClass(Class<?> base, int dims) {
        Preconditions.checkArgument(base != null, "null base");
        Preconditions.checkArgument(!base.isArray(), "base is array type");
        Preconditions.checkArgument(dims >= 0 && dims <= 255, "invalid number of array dimensions (" + dims + ")");
        Preconditions.checkArgument(base != void.class || dims == 0, "invalid void array");
        if (dims == 0)
            return base;
        final String suffix = base.isPrimitive() ? "" + Primitive.get(base).getLetter() : "L" + base.getName() + ";";
        final StringBuilder buf = new StringBuilder(dims + suffix.length());
        while (dims-- > 0)
            buf.append('[');
        buf.append(suffix);
        final String className = buf.toString();
        try {
            return Class.forName(className, false, base.getClassLoader());
        } catch (Exception e) {
            throw new RuntimeException("error loading array class `" + className + "'");
        }
    }

// HasNamePredicate

    /**
     * Predicate matching {@link SchemaItem}s by name.
     */
    public static class HasNamePredicate implements Predicate<SchemaItem> {

        private final String name;

        /**
         * Constructor.
         *
         * @param name item name
         */
        public HasNamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(SchemaItem item) {
            return item.getName().equals(this.name);
        }
    }
}

