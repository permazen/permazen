
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import io.permazen.core.CounterField;
import io.permazen.core.DeletedObjectException;
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
import io.permazen.encoding.Encoding;

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
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/prism.min.js"></script>
 * <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/components/prism-java.min.js"></script>
 * <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.27.0/themes/prism.min.css" rel="stylesheet"/>
 *
 * <p>
 * To use this class for implementing {@link Object#toString}, add a method like this to your Java model classes:
 *
 * <pre><code class="language-java">
 *  &#64;Override
 *  public String toString() {
 *      return ObjDumper.toString(this.getTransaction().getTransaction(), this.getObjId(), 16);
 *  }
 * </code></pre>
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

        // Get object type
        final ObjType type;
        try {
            type = tx.getObjType(id);
        } catch (StaleTransactionException e) {
            String typeName;
            try {
                typeName = tx.getSchemaBundle().getObjectTypeName(id.getStorageId());
            } catch (IllegalArgumentException e2) {
                typeName = String.format("type#%d");
            }
            return String.format("%s@%s [%s]", typeName, id, "stale tx");
        } catch (UnknownTypeException e) {
            return String.format("type#%d@%s [%s]", id.getStorageId(), id, "unknown type");
        } catch (DeletedObjectException e) {
            return String.format("type#%d@%s [%s]", id.getStorageId(), id, "does not exist");
        }

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
        final ObjType type = tx.getObjType(id);
        final Schema schema = type.getSchema();

        // Print headline
        writer.println(String.format("object %s type %s (schema \"%s\")", id, type.getName(), schema.getSchemaId()));
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
                    final T value = encoding.validate(tx.readSimpleField(id, field.getName(), false));
                    writer.println(value != null ? "\"" + encoding.toString(value) + "\"" : "null");
                    return null;
                }

                @Override
                public Void caseCounterField(CounterField field) {
                    writer.println("" + tx.readCounterField(id, field.getName(), false));
                    return null;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <E> Void caseSetField(SetField<E> field) {
                    this.handleCollection((NavigableSet<E>)tx.readSetField(id, field.getName(), false),
                      field.getElementField(), false);
                    return null;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <E> Void caseListField(ListField<E> field) {
                    this.handleCollection((List<E>)tx.readListField(id, field.getName(), false),
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
                        writer.println(item != null ? "\"" + encoding.toString(item) + "\"" : "null");
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
                    final NavigableMap<K, V> map = (NavigableMap<K, V>)tx.readMapField(id, field.getName(), false);
                    for (Map.Entry<K, V> entry : map.entrySet()) {
                        if (count == 0)
                            writer.println();
                        if (count >= maxCollectionEntries) {
                            writer.println(eindent + "...");
                            count++;
                            break;
                        }
                        final K key = entry.getKey();
                        final V value = entry.getValue();
                        final String keyStr = key != null ? "\"" + keyEncoding.toString(key) + "\"" : "null";
                        final String valueStr = value != null ? "\"" + valueEncoding.toString(value) + "\"" : "null";
                        writer.println(eindent + keyStr + " -> " + valueStr);
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
