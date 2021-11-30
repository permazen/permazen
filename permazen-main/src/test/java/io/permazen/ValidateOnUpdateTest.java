
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.Transaction;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.UUID;

import javax.validation.constraints.NotNull;

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
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);
        Transaction tx = db.createTransaction(schema1, 1, true);
        tx.create(10);
        tx.commit();

    // Version 2

        Permazen jdb = new Permazen(db, 2, null, Arrays.<Class<?>>asList(Foo.class));
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

    @PermazenType(storageId = 10)
    public abstract static class Foo implements JObject {

        @JField(storageId = 11)
        @NotNull
        public abstract UUID getUUID();
        public abstract void setUUID(UUID uuid);
    }
}

