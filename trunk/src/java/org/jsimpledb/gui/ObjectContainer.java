
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.vaadin.shared.ui.label.ContentMode;

import java.util.ArrayList;
import java.util.List;

import org.dellroad.stuff.spring.RetryTransaction;
import org.dellroad.stuff.vaadin7.PropertyDef;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;
import org.jsimpledb.JClass;
import org.jsimpledb.JComplexField;
import org.jsimpledb.JCounterField;
import org.jsimpledb.JField;
import org.jsimpledb.JObject;
import org.jsimpledb.JReferenceField;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

@SuppressWarnings("serial")
public class ObjectContainer extends SimpleKeyedContainer<ObjId, JObject> {

    public static final String OBJ_ID_PROPERTY = "$objId";
    public static final String TYPE_PROPERTY = "$type";
    public static final String VERSION_PROPERTY = "$version";

    private static final int MAX_OBJECTS = 1000;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final JClass<?> jclass;

    /**
     * Constructor.
     */
    public ObjectContainer(JClass<?> jclass) {
        this.jclass = jclass;
        this.setPropertyExtractor(this);
        this.setProperties(this.buildPropertyDefs());
    }

    public JClass<?> getJClass() {
        return this.jclass;
    }

    @Override
    public void connect() {
        super.connect();
        this.reload();
    }

    @Override
    public ObjId getKeyFor(JObject jobj) {
        return jobj.getObjId();
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    public void reload() {
        this.load(Iterables.limit(Iterables.transform(JTransaction.getCurrent().getAll(this.jclass.getTypeToken().getRawType()),
          new Function<Object, JObject>() {
            @Override
            public JObject apply(Object obj) {
                return ((JObject)obj).copyOut();
            }
          }), MAX_OBJECTS));
    }

// Property derivation

    private List<ObjPropertyDef<?>> buildPropertyDefs() {
        final ArrayList<ObjPropertyDef<?>> list = new ArrayList<>();

        // Add properties shared by all JObjects
        list.add(new ObjPropertyDef<SizedLabel>(OBJ_ID_PROPERTY, SizedLabel.class) {
            @Override
            public SizedLabel extract(JObject jobj) {
                final SizedLabel label = new SizedLabel("<code>" + jobj.getObjId().toString() + "</code>", ContentMode.HTML);
                return label;
            }
        });
        list.add(new ObjPropertyDef<String>(TYPE_PROPERTY, String.class) {
            @Override
            public String extract(JObject jobj) {
                return jobj.getTransaction().getJSimpleDB().getJClass(jobj.getObjId().getStorageId()).getName();
            }
        });
        list.add(new ObjPropertyDef<Integer>(VERSION_PROPERTY, Integer.class, 0) {
            @Override
            public Integer extract(JObject jobj) {
                return jobj.getSchemaVersion();
            }
        });

        // Add type-specific properties
        for (JField jfield0 : this.jclass.getJFieldsByName().values()) {
            final JField jfield = jfield0;
            final ObjPropertyDef<?> propertyDef;
            if (jfield instanceof JCounterField) {
                propertyDef = new ObjPropertyDef<Long>(jfield.getName(), Long.class, 0L) {
                    @Override
                    public Long extract(JObject jobj) {
                        return jobj.getTransaction().readCounterField(jobj.getObjId(), jfield.getStorageId()).get();
                    }
                };
            } else if (jfield instanceof JComplexField) {
                propertyDef = new ObjPropertyDef<String>(jfield.getName(), String.class) {
                    @Override
                    public String extract(JObject jobj) {
                        return "[TODO]";
                    }
                };
            } else if (jfield instanceof JReferenceField) {
                propertyDef = new ObjPropertyDef<String>(jfield.getName(), String.class) {
                    @Override
                    public String extract(JObject jobj) {
                        return "[TODO]";
                    }
                };
            } else if (jfield instanceof JSimpleField) {
                propertyDef = new ObjPropertyDef<String>(jfield.getName(), String.class) {
                    @Override
                public String extract(JObject jobj) {
                        final ObjId id = jobj.getObjId();
                        final Transaction tx = jobj.getTransaction().getTransaction();
                        final ObjType objType = tx.getSchema().getVersion(tx.getSchemaVersion(id))
                          .getSchemaItem(id.getStorageId(), ObjType.class);
                        final SimpleField<?> field = (SimpleField<?>)objType.getFields().get(jfield.getStorageId());
                        final FieldType<?> fieldType = field.getFieldType();
                        return this.toString(fieldType, tx.readSimpleField(id, field.getStorageId(), false));
                    }

                    // This method exists solely to bind the generic type parameters
                    private <T> String toString(FieldType<T> fieldType, Object value) {
                        return fieldType.toString(fieldType.validate(value));
                    }
                };
            } else
                throw new RuntimeException("internal error");
            list.add(propertyDef);
        }

        // Done
        return list;
    }

// PropertyExtractor

    @Override
    public <V> V getPropertyValue(JObject jobj, PropertyDef<V> propertyDef) {
        return ((ObjPropertyDef<V>)propertyDef).extract(jobj);
    }

// ObjPropertyDef

    private abstract static class ObjPropertyDef<V> extends PropertyDef<V> {

        ObjPropertyDef(String name, Class<V> type) {
            super(name, type);
        }

        ObjPropertyDef(String name, Class<V> type, V defaultValue) {
            super(name, type, defaultValue);
        }

        public abstract V extract(JObject jobj);
    }
}

