
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;

import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;

public class ObjectItemType extends FieldTypeItemType<ObjId> {

    public ObjectItemType(Session session) {
        super(session, ObjId.class);
    }

    @Override
    public void print(Session session, ObjId id, boolean verbose) {

        // Get console writer
        final PrintWriter writer = session.getWriter();

        // Verify object exists
        final Transaction tx = session.getTransaction();
        final Util.ObjInfo info = Util.getObjInfo(session, id);
        if (info == null)
            writer.println("object " + id + " (does not exist)");

        // Print headline
        writer.println("object " + info);
        if (!verbose)
            return;

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
            if (field instanceof SimpleField)
                writer.println(this.readSimpleFieldStringValue(tx, (SimpleField<?>)field, id));
            else if (field instanceof CounterField)
                writer.println("" + tx.readCounterField(id, field.getStorageId(), false));
            else if (field instanceof SetField) {
                final SimpleField<?> elementField = ((SetField<?>)field).getElementField();
                writer.println("{");
                int count = 0;
                for (Object elem : tx.readSetField(id, field.getStorageId(), false)) {
                    if (count >= session.getLineLimit()) {
                        writer.println(eindent + "...");
                        break;
                    }
                    writer.println(eindent + this.getSimpleFieldStringValue(elementField, elem));
                    count++;
                }
                writer.println(indent + "}");
            } else if (field instanceof ListField) {
                final SimpleField<?> elementField = ((ListField<?>)field).getElementField();
                writer.println("{");
                int count = 0;
                for (Object elem : tx.readListField(id, field.getStorageId(), false)) {
                    if (count >= session.getLineLimit()) {
                        writer.println(eindent + "...");
                        break;
                    }
                    writer.println(eindent + "[" + count + "]" + this.getSimpleFieldStringValue(elementField, elem));
                    count++;
                }
                writer.println(indent + "}");
            } else if (field instanceof MapField) {
                final SimpleField<?> keyField = ((MapField<?, ?>)field).getKeyField();
                final SimpleField<?> valueField = ((MapField<?, ?>)field).getValueField();
                writer.println("{");
                int count = 0;
                for (Map.Entry<?, ?> entry : tx.readMapField(id, field.getStorageId(), false).entrySet()) {
                    if (count >= session.getLineLimit()) {
                        writer.println(eindent + "...");
                        break;
                    }
                    writer.println(eindent + this.getSimpleFieldStringValue(keyField, entry.getKey())
                      + " -> " + this.getSimpleFieldStringValue(valueField, entry.getValue()));
                    count++;
                }
                writer.println(indent + "}");
            } else
                throw new RuntimeException("internal error");
        }
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("unchecked")
    private <T> String readSimpleFieldStringValue(Transaction tx, SimpleField<T> field, ObjId id) {
        return field.getFieldType().toString((T)tx.readSimpleField(id, field.getStorageId(), false));
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("unchecked")
    private <T> String getSimpleFieldStringValue(SimpleField<T> field, Object value) {
        return field.getFieldType().toString((T)value);
    }
}

