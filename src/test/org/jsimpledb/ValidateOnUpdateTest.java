
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.UUID;

import javax.validation.constraints.NotNull;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.schema.SchemaModel;
import org.testng.annotations.Test;

public class ValidateOnUpdateTest extends TestSupport {

    @Test
    public void testValidateOnUpdate() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

    // Version 1

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"1\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "    <SimpleField name=\"uuid\" storageId=\"11\" type=\"java.util.UUID\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final Database db = new Database(kvstore);
        Transaction tx = db.createTransaction(schema1, 1, true);
        tx.create(10);
        tx.commit();

    // Version 2

        JSimpleDB jdb = new JSimpleDB(db, 2, null, Arrays.<Class<?>>asList(Foo.class));
        JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {
            final Foo foo = jtx.getAll(Foo.class).iterator().next();
            foo.upgrade();
            try {
                jtx.commit();                    // should fail because UUID is null
                assert false;
            } catch (ValidationException e) {
                // expected
            }
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @JSimpleClass(storageId = 10)
    public abstract static class Foo implements JObject {

        @JField(storageId = 11)
        @NotNull
        public abstract UUID getUUID();
        public abstract void setUUID(UUID uuid);
    }
}

