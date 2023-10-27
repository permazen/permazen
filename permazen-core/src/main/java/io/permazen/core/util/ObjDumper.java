
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import io.permazen.core.CounterField;
import io.permazen.core.DeletedObjectException;
import io.permazen.core.Encoding;
import io.permazen.core.Field;
import io.permazen.core.FieldSwitch;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.ObjType;
import io.permazen.core.Schema;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.StaleTransactionException;
import io.permazen.core.Transaction;
import io.permazen.core.UnknownTypeException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Utility classes for printing database objects and fields in a human-readable format.
 *
 * <p>
 * To use this class for implementing {@link Object#toString}, add a method like this to your Java model classes:
 * <blockquote><pre>
 *  &#64;Override
 *  public String toString() {
 *      return ObjDumper.toString(this.getTransaction().getTransaction(), this.getObjId(), 16);
 *  }
 * </pre></blockquote>
 */
public final class ObjDumper {

    private ObjDumper() {
    }

    /**
     * Helper for Java model object {@link Object#toString} methods that wish to display all fields in the object.
     *
     * @param tx transaction containing the object
     * @param id the ID of the object
     * @param maxCollectionEntries maximum number of elements to display in any collection field or -1 to not display any fields
     * @return contents of the specified object, or just object type and ID if {@code tx} is no longer valid
     * @throws IllegalArgumentException if {@code tx} or {@code id} is null
     */
    public static String toString(Transaction tx, ObjId id, int maxCollectionEntries) {

        // Sanity check
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(id != null, "null id");

        // Get object info
        /*final*/ Schema schema;
        boolean deleted = false;
        try {
            schema = tx.getSchemas().getVersion(tx.getSchemaVersion(id));
        } catch (IllegalArgumentException | StaleTransactionException | DeletedObjectException e) {
            schema = tx.getSchema();
            deleted = e instanceof DeletedObjectException;
        }
        final ObjType type;
        try {
            type = schema.getObjType(id.getStorageId());
        } catch (UnknownTypeException e) {
            return "type#" + id.getStorageId() + "@" + id + " [unknown type]";
        }

        // Is transaction valid?
        if (!tx.isValid())
            return type.getName() + "@" + id + " [stale tx]";

        // Is object deleted?
        if (deleted)
            return type.getName() + "@" + id + " [does not exist]";

        // Format object
        final StringWriter buf = new StringWriter();
        try {
            ObjDumper.print(new PrintWriter(buf), tx, id, maxCollectionEntries);
        } catch (Exception e) {
            buf.write("[exception reading " + type.getName() + "@" + id + ": " + e + "]");
        }

        // Done
        return buf.toString().trim();
    }

    /**
     * Print the content of the given object's fields in a human readable format.
     *
     * <p>
     * The given transaction must still be open and the specified object must exist therein.
     *
     * @param writer output destination
     * @param tx transaction containing the object
     * @param id the ID of the object
     * @param maxCollectionEntries maximum number of elements to display in any collection field or -1 to not display any fields
     * @throws DeletedObjectException if the object does not exist in {@code tx}
     * @throws StaleTransactionException if {@code tx} is no longer usable
     * @throws UnknownTypeException if {@code id} specifies an unknown object type
     * @throws IllegalArgumentException if any parameter is null
     */
    public static void print(final PrintWriter writer, final Transaction tx, final ObjId id, final int maxCollectionEntries) {

        // Sanity check
        Preconditions.checkArgument(writer != null, "null writer");
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(id != null, "null id");

        // Get object info
        final int schemaVersion = tx.getSchemaVersion(id);
        final Schema schema = tx.getSchemas().getVersion(schemaVersion);
        final ObjType type = schema.getObjType(id.getStorageId());

        // Print headline
        writer.println("object " + id + " type " + type.getName() + "#" + type.getStorageId() + " version " + schemaVersion);
        if (maxCollectionEntries < 0) {
            writer.flush();
            return;
        }

        // Calculate indent amount
        int nameFieldSize = 0;
        for (Field<?> field : type.getFields().values())
            nameFieldSize = Math.max(nameFieldSize, field.getName().length());
        final char[] ichars = new char[nameFieldSize + 3];
        Arrays.fill(ichars, ' ');
        final String indent = new String(ichars);
        final String eindent = indent + "  ";

        // Display fields
        for (Field<?> field : type.getFields().values()) {
            writer.print(String.format("%" + nameFieldSize + "s = ", field.getName()));
            field.visit(new FieldSwitch<Void>() {

                @Override
                public <T> Void caseSimpleField(SimpleField<T> field) {
                    final Encoding<T> encoding = field.getEncoding();
                    writer.println(encoding.toParseableString(
                      encoding.validate(tx.readSimpleField(id, field.getStorageId(), false))));
                    return null;
                }

                @Override
                public Void caseCounterField(CounterField field) {
                    writer.println("" + tx.readCounterField(id, field.getStorageId(), false));
                    return null;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <E> Void caseSetField(SetField<E> field) {
                    this.handleCollection((NavigableSet<E>)tx.readSetField(id, field.getStorageId(), false),
                      field.getElementField(), false);
                    return null;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <E> Void caseListField(ListField<E> field) {
                    this.handleCollection((List<E>)tx.readListField(id, field.getStorageId(), false),
                      field.getElementField(), true);
                    return null;
                }

                private <E> void handleCollection(Collection<E> items, SimpleField<E> elementField, boolean showIndex) {
                    final Encoding<E> encoding = elementField.getEncoding();
                    writer.print("{");
                    int count = 0;
                    for (E item : items) {
                        if (count == 0)
                            writer.println();
                        if (count >= maxCollectionEntries) {
                            writer.println(eindent + "...");
                            count++;
                            break;
                        }
                        writer.print(eindent);
                        if (showIndex)
                            writer.print("[" + count + "] ");
                        writer.println(encoding.toParseableString(item));
                        count++;
                    }
                    writer.println(count == 0 ? " }" : indent + "}");
                }

                @Override
                @SuppressWarnings("unchecked")
                public <K, V> Void caseMapField(MapField<K, V> field) {
                    final Encoding<K> keyEncoding = field.getKeyField().getEncoding();
                    final Encoding<V> valueEncoding = field.getValueField().getEncoding();
                    writer.print("{");
                    int count = 0;
                    final NavigableMap<K, V> map = (NavigableMap<K, V>)tx.readMapField(id, field.getStorageId(), false);
                    for (Map.Entry<K, V> entry : map.entrySet()) {
                        if (count == 0)
                            writer.println();
                        if (count >= maxCollectionEntries) {
                            writer.println(eindent + "...");
                            count++;
                            break;
                        }
                        writer.println(eindent + keyEncoding.toParseableString(entry.getKey())
                          + " -> " + valueEncoding.toParseableString(entry.getValue()));
                        count++;
                    }
                    writer.println(count == 0 ? " }" : indent + "}");
                    return null;
                }
            });
        }
        writer.flush();
    }
}
