
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.func;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.JObject;
import org.jsimpledb.cli.ObjInfo;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.parse.expr.EvalException;
import org.jsimpledb.cli.parse.expr.Value;
import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldSwitchAdapter;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;

@CliFunction
public class DumpFunction extends SimpleFunction {

    public DumpFunction() {
        super("dump", 1, 1);
    }

    @Override
    public String getHelpSummary() {
        return "prints all fields of the given object to the console";
    }

    @Override
    public String getUsage() {
        return "dump(expr)";
    }

    @Override
    protected Value apply(Session session, Value[] params) {

        // Get object
        Object obj = params[0].checkNotNull(session, "dump()");
        if (obj instanceof JObject)
            obj = ((JObject)obj).getObjId();
        else if (!(obj instanceof ObjId))
            throw new EvalException("invalid dump() operation on non-database object of type " + obj.getClass().getName());
        final ObjId id = (ObjId)obj;

        // Dump object
        this.dump(session, id);

        // Done
        return new Value(null);
    }

    private void dump(final Session session, final ObjId id) {

        // Get transaction and console writer
        final Transaction tx = session.getTransaction();
        final PrintWriter writer = session.getWriter();

        // Verify object exists
        final ObjInfo info = ObjInfo.getObjInfo(session, id);
        if (info == null)
            writer.println("object " + id + " (does not exist)");

        // Print headline
        writer.println("object " + info);

        // Calculate indent amount
        int nameFieldSize = 0;
        for (Field<?> field : info.getObjType().getFields().values())
            nameFieldSize = Math.max(nameFieldSize, field.getName().length());
        final char[] ichars = new char[nameFieldSize + 3];
        Arrays.fill(ichars, ' ');
        final String indent = new String(ichars);
        final String eindent = indent + "  ";

        // Display fields
        for (Field<?> field : info.getObjType().getFields().values()) {
            writer.print(String.format("%" + nameFieldSize + "s = ", field.getName()));
            field.visit(new FieldSwitchAdapter<Void>() {

                @Override
                public <T> Void caseSimpleField(SimpleField<T> field) {
                    final FieldType<T> fieldType = field.getFieldType();
                    writer.println(fieldType.toString(fieldType.validate(tx.readSimpleField(id, field.getStorageId(), false))));
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
                        if (count >= session.getLineLimit()) {
                            writer.println(eindent + "...");
                            break;
                        }
                        writer.print(eindent);
                        if (showIndex)
                            writer.print("[" + count + "] ");
                        writer.println(fieldType.toString(item));
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
                        if (count >= session.getLineLimit()) {
                            writer.println(eindent + "...");
                            break;
                        }
                        writer.println(eindent + keyFieldType.toString(entry.getKey())
                          + " -> " + valueFieldType.toString(entry.getValue()));
                        count++;
                    }
                    writer.println(indent + "}");
                    return null;
                }
            });
        }
    }
}

