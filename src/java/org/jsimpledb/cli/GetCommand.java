
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import java.util.Map;

import org.jsimpledb.core.ComplexField;
import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.ReferenceField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.util.ParseContext;

public class GetCommand extends Command {

    public GetCommand() {
        super("get type:type field:field");
    }

    @Override
    public String getHelpSummary() {
        return "gets a field from incoming objects";
    }

    @Override
    public String getHelpDetail() {
        return "The 'get' command takes two arguments, a type name (or storage ID) and a field name. The top channel, which"
          + " must contain objects, is replaced with a channel containing the content of the specified field in those objects."
          + " If an object is encountered that does not have the specified field, an error occurs.";
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        if ("field".equals(typeName))
            return new FieldParser();
        return super.getParser(typeName);
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Get params
        final ObjType objType = (ObjType)params.get("type");
        final Field<?> field = (Field<?>)params.get("field");

        // Return action
        return new Action() {

            @Override
            public void run(Session session) {
                final Channel<? extends ObjId> channel = GetCommand.this.pop(session, ObjId.class);
                final TypeToken<?> typeToken = field.getTypeToken();
                final Channel<?> result;
                if (field instanceof ReferenceField)
                    result = this.getValues(session, channel, (ReferenceField)field);
                else if (field instanceof CounterField)
                    result = this.getValues(session, channel, (CounterField)field);
                else if (field instanceof SimpleField)
                    result = this.getValues(session, channel, (SimpleField<?>)field);
                else
                    result = this.getValues(session, channel, (ComplexField<?>)field);
                GetCommand.this.push(session, result);
            }

            // References
            private Channel<ObjId> getValues(Session session, final Channel<? extends ObjId> channel, final ReferenceField field) {
                return new TransformItemsChannel<ObjId, ObjId>(channel, new ObjectItemType(session)) {
                    @Override
                    protected ObjId transformItem(Session session, ObjId id) {
                        return field.getValue(session.getTransaction(), id);
                    }
                };
            }

            // Counters
            private Channel<Long> getValues(Session session, Channel<? extends ObjId> channel, final CounterField field) {
                return new TransformItemsChannel<ObjId, Long>(channel, Long.class) {
                    @Override
                    protected Long transformItem(Session session, ObjId id) {
                        return field.getValue(session.getTransaction(), id);
                    }
                };
            }

            // Simple fields
            private <T> Channel<T> getValues(Session session, Channel<? extends ObjId> channel, final SimpleField<T> field) {
                final FieldType<T> fieldType = field.getFieldType();
                return new TransformItemsChannel<ObjId, T>(channel, fieldType) {
                    @Override
                    protected T transformItem(Session session, ObjId id) {
                        return field.getValue(session.getTransaction(), id);
                    }
                };
            }

            // Complex fields
            private <T> Channel<T> getValues(Session session, Channel<? extends ObjId> channel, final ComplexField<T> field) {
                return new TransformItemsChannel<ObjId, T>(channel, field.getTypeToken()) {
                    @Override
                    protected T transformItem(Session session, ObjId id) {
                        return field.getValue(session.getTransaction(), id);
                    }
                };
            }
        };
    }

// FieldParser

    private class FieldParser implements Parser<Field<?>> {

        @Override
        public Field<?> parse(Session session, ParseContext ctx, boolean complete) {

            // Get object type already parsed
            final ObjType objType = (ObjType)ParamParser.getParametersAlreadyParsed().get("type");

            // Find the field
            final String fieldName = ctx.matchPrefix("[^\\s;]*").group();
            final Field<?> field = Iterables.find(objType.getFields().values(), new Predicate<Field<?>>() {
                @Override
                public boolean apply(Field<?> field) {
                    return fieldName.equals(field.getName()) || fieldName.equals("" + field.getStorageId());
                }
              }, null);
            if (field == null) {
                throw new ParseException(ctx, "no such field `" + fieldName + "' in " + objType).addCompletions(
                  Util.complete(Iterables.transform(objType.getFields().values(), new Function<Field<?>, String>() {
                      @Override
                      public String apply(Field<?> field) {
                          return field.getName();
                      }
                  }), fieldName));
            }

            // Done
            return field;
        }
    }
}

