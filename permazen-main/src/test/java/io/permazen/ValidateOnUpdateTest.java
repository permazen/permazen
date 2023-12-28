
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;

import jakarta.validation.constraints.NotNull;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

import org.testng.annotations.Test;

public class ValidateOnUpdateTest extends MainTestSupport {

    @Test
    public void testValidateOnUpdate() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

    // Version 1

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\">\n"
          + "    <SimpleField name=\"uuid\" encoding=\"urn:fdc:permazen.io:2020:UUID\"/>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);
        final TransactionConfig txConfig1 = TransactionConfig.builder()
          .schemaModel(schema1)
          .build();

        Transaction tx = db.createTransaction(txConfig1);
        tx.create("Foo");
        tx.commit();

    // Version 2

        Permazen jdb = BasicTest.newPermazen(db, Foo.class);
        JTransaction jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {
            final Foo foo = jtx.getAll(Foo.class).iterator().next();
            foo.migrateSchema();
            try {
                jtx.commit();                    // should fail because UUID is null
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public abstract static class Foo implements JObject {

        @JField(name = "uuid")
        @NotNull
        public abstract UUID getUUID();
        public abstract void setUUID(UUID uuid);

        public abstract Set<Integer> getDummy();    // add a field to force a schema change
    }
}
