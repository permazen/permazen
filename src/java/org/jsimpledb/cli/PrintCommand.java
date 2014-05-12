
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.io.IOException;
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
import org.jsimpledb.util.ParseContext;

public class PrintCommand extends Command {

    public PrintCommand(AggregateCommand parent) {
        super(parent, "print");
    }

    @Override
    public Action parse(Session session, ParseContext ctx) throws ParseException {
        final ObjId id = Util.parseObjId(session, ctx, this.getFullName() + " object-id");
        return new TransactionAction() {
            @Override
            public void run(Session session) throws Exception {
                PrintCommand.this.printObject(session, id);
            }
        };
    }

    @Override
    public String getHelpSummary() {
        return "Prints an object's schema version and fields";
    }

    private void printObject(Session session, ObjId id) throws IOException {

        // Verify object exists
        final Transaction tx = session.getTransaction();
        final Util.ObjInfo info = Util.getObjInfo(tx, id);
        if (info == null) {
            session.getConsole().println("object " + id + " does not exist");
            return;
        }

        // Display object meta-data
        session.getConsole().println("object " + info);

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
            session.getConsole().print(String.format("%" + nameFieldSize + "s = ", field.getName()));
            if (field instanceof SimpleField)
                session.getConsole().println(this.readSimpleFieldStringValue(tx, (SimpleField<?>)field, id));
            else if (field instanceof CounterField)
                session.getConsole().println("" + tx.readCounterField(id, field.getStorageId(), false));
            else if (field instanceof SetField) {
                final SimpleField<?> elementField = ((SetField<?>)field).getElementField();
                session.getConsole().println("{");
                int count = 0;
                for (Object elem : tx.readSetField(id, field.getStorageId(), false)) {
                    if (count >= session.getLineLimit()) {
                        session.getConsole().println(eindent + "...");
                        break;
                    }
                    session.getConsole().println(eindent + this.getSimpleFieldStringValue(elementField, elem));
                    count++;
                }
                session.getConsole().println(indent + "}");
            } else if (field instanceof ListField) {
                final SimpleField<?> elementField = ((ListField<?>)field).getElementField();
                session.getConsole().println("{");
                int count = 0;
                for (Object elem : tx.readListField(id, field.getStorageId(), false)) {
                    if (count >= session.getLineLimit()) {
                        session.getConsole().println(eindent + "...");
                        break;
                    }
                    session.getConsole().println(eindent + "[" + count + "]" + this.getSimpleFieldStringValue(elementField, elem));
                    count++;
                }
                session.getConsole().println(indent + "}");
            } else if (field instanceof MapField) {
                final SimpleField<?> keyField = ((MapField<?, ?>)field).getKeyField();
                final SimpleField<?> valueField = ((MapField<?, ?>)field).getValueField();
                session.getConsole().println("{");
                int count = 0;
                for (Map.Entry<?, ?> entry : tx.readMapField(id, field.getStorageId(), false).entrySet()) {
                    if (count >= session.getLineLimit()) {
                        session.getConsole().println(eindent + "...");
                        break;
                    }
                    session.getConsole().println(eindent + this.getSimpleFieldStringValue(keyField, entry.getKey())
                      + " -> " + this.getSimpleFieldStringValue(valueField, entry.getValue()));
                    count++;
                }
                session.getConsole().println(indent + "}");
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

