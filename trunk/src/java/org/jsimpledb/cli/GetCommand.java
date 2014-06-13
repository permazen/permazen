
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.Map;

import org.jsimpledb.core.Field;
import org.jsimpledb.core.FieldSwitchAdapter;
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
            public void run(final Session session) {
                final Channel<? extends ObjId> channel = GetCommand.this.pop(session, ObjId.class);
                GetCommand.this.push(session, field.visit(new FieldSwitchAdapter<Channel<?>>() {

                    @Override
                    public Channel<?> caseReferenceField(final ReferenceField field) {
                        return new TransformItemsChannel<ObjId, ObjId>(channel, new ObjectItemType(session)) {
                            @Override
                            protected ObjId transformItem(Session session, ObjId id) {
                                return field.getValue(session.getTransaction(), id);
                            }
                        };
                    }

                    @Override
                    public <T> Channel<?> caseSimpleField(final SimpleField<T> field) {
                        final FieldType<T> fieldType = field.getFieldType();
                        return new TransformItemsChannel<ObjId, T>(channel, fieldType) {
                            @Override
                            protected T transformItem(Session session, ObjId id) {
                                return field.getValue(session.getTransaction(), id);
                            }
                        };
                    }

                    @Override
                    public <T> Channel<?> caseField(final Field<T> field) {
                        return new TransformItemsChannel<ObjId, T>(channel, field.getTypeToken()) {
                            @Override
                            protected T transformItem(Session session, ObjId id) {
                                return field.getValue(session.getTransaction(), id);
                            }
                        };
                    }
                }));
            }
        };
    }
}

