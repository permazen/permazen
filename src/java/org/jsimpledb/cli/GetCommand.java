
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.reflect.TypeToken;

import java.util.Map;

import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferenceField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.util.ParseContext;

public class GetCommand extends Command {

    public GetCommand() {
        super("get field:field");
    }

    @Override
    public String getHelpSummary() {
        return "gets a field from incoming objects";
    }

    @Override
    public String getHelpDetail() {
        return "The 'get' command takes one argument which consists of a type name, period, and field name. The top channel, which"
          + " must contain objects, is replaced with a channel containing the content of the specified field in those objects."
          + " If an object is encountered that does not have the specified field, an error occurs.";
    }

    @Override
    public Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Get params
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
                else if (field instanceof SimpleField)
                    result = this.getValues(session, channel, (SimpleField<?>)field);
                else
                    result = this.getValues(session, channel, field);
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

            // Other fields
            private <T> Channel<T> getValues(Session session, Channel<? extends ObjId> channel, final Field<T> field) {
                return new TransformItemsChannel<ObjId, T>(channel, field.getTypeToken()) {
                    @Override
                    protected T transformItem(Session session, ObjId id) {
                        return field.getValue(session.getTransaction(), id);
                    }
                };
            }
        };
    }
}

