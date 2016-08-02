
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.util;

import com.google.common.base.Preconditions;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldSwitchAdapter;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.Schema;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.StaleTransactionException;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.core.UnknownTypeException;

/**
 * Utility classes for printing database objects and fields in a human-readable format.
 *
 * <p>
 * To use this class for implementing {@link Object#toString}, add a method like this to your Java model classes:
 * <blockquote><pre>
 *  @Override
 *  public String toString() {
 *      return ObjDumper.toString(this.getTransaction().getTransaction(), this.getObjId(), 16);
 *  }
 * </pre></blockquote>
 */
public final class ObjDumper {

    private ObjDumper() {
    }

    /**
     * Helper method for implementations of {@link JObject#toString} that wish to display all fields in an object.
     *
     * @param tx transaction containing the object
     * @param id the ID of the object
     * @param maxCollectionEntries maximum number of elements to display in any collection field
     * @return contents of the specified object, or just object type and ID if {@code tx} is no longer valid
     * @throws IllegalArgumentException if {@code tx} or {@code id} is null
     */
    public static String toString(Transaction tx, ObjId id, int maxCollectionEntries) {

        // Sanity check
        Preconditions.checkArgument(tx != null, "null tx");
        Preconditions.checkArgument(id != null, "null id");

        // Get object info
        /*final*/ Schema schema;
        try {
            schema = tx.getSchemas().getVersion(tx.getSchemaVersion(id));
        } catch (IllegalArgumentException | StaleTransactionException | DeletedObjectException e) {
            schema = tx.getSchema();
        }
        final ObjType type;
        try {
            type = schema.getObjType(id.getStorageId());
        } catch (UnknownTypeException e) {
            return "type#" + id.getStorageId() + "@" + id;
        }

        // Is transaction valid?
        if (!tx.isValid())
            return type.getName() + "@" + id + " [stale tx]";

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
     * @param maxCollectionEntries maximum number of elements to display in any collection field
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
            field.visit(new FieldSwitchAdapter<Void>() {

                @Override
                public <T> Void caseSimpleField(SimpleField<T> field) {
                    final FieldType<T> fieldType = field.getFieldType();
                    writer.println(fieldType.toParseableString(
                      fieldType.validate(tx.readSimpleField(id, field.getStorageId(), false))));
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
                    final FieldType<E> fieldType = elementField.getFieldType();
                    writer.println("{");
                    int count = 0;
                    for (E item : items) {
                        if (count >= maxCollectionEntries) {
                            writer.println(eindent + "...");
                            break;
                        }
                        writer.print(eindent);
                        if (showIndex)
                            writer.print("[" + count + "] ");
                        writer.println(fieldType.toParseableString(item));
                        count++;
                    }
                    writer.println(indent + "}");
                }

                @Override
                @SuppressWarnings("unchecked")
                public <K, V> Void caseMapField(MapField<K, V> field) {
                    final FieldType<K> keyFieldType = field.getKeyField().getFieldType();
                    final FieldType<V> valueFieldType = field.getValueField().getFieldType();
                    writer.println("{");
                    int count = 0;
                    final NavigableMap<K, V> map = (NavigableMap<K, V>)tx.readMapField(id, field.getStorageId(), false);
                    for (Map.Entry<K, V> entry : map.entrySet()) {
                        if (count >= maxCollectionEntries) {
                            writer.println(eindent + "...");
                            break;
                        }
                        writer.println(eindent + keyFieldType.toParseableString(entry.getKey())
                          + " -> " + valueFieldType.toParseableString(entry.getValue()));
                        count++;
                    }
                    writer.println(indent + "}");
                    return null;
                }
            });
        }
        writer.flush();
    }
}
