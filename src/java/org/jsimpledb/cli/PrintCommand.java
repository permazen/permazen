
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.reflect.TypeToken;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class PrintCommand extends AbstractTransformChannelCommand<CommandParser> {

    public PrintCommand() {
        super("print");
    }

    @Override
    public String getUsage() {
        return this.name + " [-v]";
    }

    @Override
    public String getHelpSummary() {
        return "prints data items to the console";
    }

    @Override
    public String getHelpDetail() {
        return "Prints data items to the console. This is the default final action.";
    }

    @Override
    protected CommandParser getParameters(Session session, Channels channels, ParseContext ctx) {
        return new CommandParser(0, 0, this.getUsage(), "-v").parse(ctx);
    }

    @Override
    protected <T> Channel<String> transformChannel(Session session, Channel<T> channel, final CommandParser parser) {

        // Handle objects
        final TypeToken<T> typeToken = channel.getItemType().getTypeToken();
        if (typeToken.equals(TypeToken.of(ObjId.class))) {
            return new TransformItemsChannel<T, String>(channel, String.class) {
                @Override
                protected String transformItem(Session session, T value) {
                    return PrintCommand.this.printObject(session, (ObjId)value, parser.hasFlag("-v"));
                }
            };
        }

        // Handle field types
        final List<FieldType<T>> fieldTypes = session.getDatabase().getFieldTypeRegistry().getFieldTypes(typeToken);
        if (!fieldTypes.isEmpty()) {
            final FieldType<T> fieldType = fieldTypes.get(0);
            return new TransformItemsChannel<T, String>(channel, String.class) {
                @Override
                protected String transformItem(Session session, T value) {
                    return fieldType.toString(value);
                }
            };
        }

        // Handle whatever else
        return new TransformItemsChannel<T, String>(channel, String.class) {
            @Override
            protected String transformItem(Session session, T value) {
                return "" + value;
            }
        };
    }

    private String printObject(Session session, ObjId id, boolean verbose) {

        // Verify object exists
        final Transaction tx = session.getTransaction();
        final Util.ObjInfo info = Util.getObjInfo(session, id);
        if (info == null)
            return "object " + id + " does not exist";

        // Get headline
        final String headline = "object " + info;

        // Quiet mode?
        if (!verbose)
            return headline;

        // Setup buffer
        final StringWriter buf = new StringWriter();
        final PrintWriter writer = new PrintWriter(buf);
        writer.println(headline);

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

        // Done - trim trailing whitespace
        writer.flush();
        return buf.toString().replaceAll("\\s+$", "");
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

