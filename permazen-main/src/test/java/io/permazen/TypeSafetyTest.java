
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnSchemaChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.core.TypeNotInSchemaException;
import io.permazen.kv.simple.MemoryKVDatabase;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TypeSafetyTest extends MainTestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testTypeSafety() throws Exception {

    // Version 1

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "    <SimpleField name=\"ival\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"11\"/>\n"
          + "    <ReferenceField name=\"bar\" storageId=\"12\">\n"
          + "      <ObjectTypes>\n"
          + "        <ObjectType name=\"Bar\"/>\n"
          + "      </ObjectTypes>\n"
          + "    </ReferenceField>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "    <ReferenceField name=\"friend\" storageId=\"21\"/>\n"
          + "    <SimpleField name=\"fix\" encoding=\"urn:fdc:permazen.io:2020:boolean\" storageId=\"22\"/>\n"
          + "    <SetField name=\"set\" storageId=\"23\">\n"
          + "      <ReferenceField storageId=\"24\">\n"
          + "        <ObjectTypes>\n"
          + "          <ObjectType name=\"Foo\"/>\n"
          + "          <ObjectType name=\"Bar\"/>\n"
          + "        </ObjectTypes>\n"
          + "      </ReferenceField>\n"
          + "    </SetField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));
        schema1.lockDown(true);
        final SchemaId schemaId1 = schema1.getSchemaId();

        final Database db = new Database(new MemoryKVDatabase());
        final TransactionConfig txConfig1 = TransactionConfig.builder()
          .schemaModel(schema1)
          .build();
        Transaction tx = db.createTransaction(txConfig1);

        final ObjId f1 = tx.create("Foo");
        final ObjId f2 = tx.create("Foo");
        final ObjId b1 = tx.create("Bar");
        final ObjId b2 = tx.create("Bar");
        final ObjId b3 = tx.create("Bar");

        tx.writeSimpleField(b1, "friend", f1, false);                           // bar1.setFriend(foo1)
        tx.writeSimpleField(f1, "ival", 1234, false);                           // foo1.setIVal(1234)
        tx.writeSimpleField(f1, "bar", b2, false);                              // foo1.setBar(bar2)

        final NavigableSet<ObjId> bar3set = (NavigableSet<ObjId>)tx.readSetField(b3, "set", false);
        tx.writeSimpleField(b3, "friend", f2, false);                           // bar3.setFriend(foo2)
        bar3set.add(b1);
        bar3set.add(b2);
        bar3set.add(f1);
        bar3set.add(f2);

        // Attempt to set disallowed references
        try {
            tx.writeSimpleField(f1, "bar", f1, false);                          // foo1.setBar(foo1)
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected exception: {}", e.toString());
        }
        Assert.assertEquals(tx.readSimpleField(f1, "bar", false), b2);          // verify foo1.getBar() == bar2

        tx.commit();

    /*

        At this point:

            foo1.ival == 1234
            foo1.bar == bar2

            bar1.friend = foo1
            bar1.set = { }
            bar2.set = { foo1 }

            bar3.friend = foo2
            bar3.set = { foo1, foo2, bar1, bar2 }

    */

    // Version 2

        Permazen pdb = BasicTest.newPermazen(db, Bar.class);                // note: no Foo.class, only Bar.class
        final SchemaId schemaId2 = pdb.getSchemaModel().getSchemaId();
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            // Get objects
            final PermazenObject foo1 = ptx.get(f1, PermazenObject.class);
            final PermazenObject foo2 = ptx.get(f2, PermazenObject.class);
            final Bar bar1 = ptx.get(b1, Bar.class);
            final Bar bar2 = ptx.get(b2, Bar.class);
            final Bar bar3 = ptx.get(b3, Bar.class);

            // Verify index on Bar.friend does not contain any type "Foo" keys
            final NavigableMap<Bar, NavigableSet<Bar>> friendIndex = ptx.querySimpleIndex(Bar.class, "friend", Bar.class).asMap();
            friendIndex.keySet().stream().filter(key -> key != null).iterator().forEachRemaining(Bar::dummy);

            // Verify index on Bar.set.element does not contain any type "Foo" keys
            final NavigableMap<Bar, NavigableSet<Bar>> setIndex = ptx.querySimpleIndex(Bar.class, "set.element", Bar.class).asMap();
            setIndex.keySet().stream().filter(key -> key != null).iterator().forEachRemaining(Bar::dummy);

            // Verify bar1 has wrongly type'd field prior to upgrade
            Assert.assertEquals(ptx.getTransaction().readSimpleField(b1, "friend", false), f1);

            // Configure bar1 to update the field on version update
            ptx.getTransaction().writeSimpleField(bar1.getObjId(), "fix", true, false);     // bar1.setFix(true)

            // Attempt to read wrongly type'd field after upgrade migration
            final Bar friend = bar1.getFriend();
            Assert.assertSame(friend, bar2);

            // Try to assign invalid Foo typed value to bar1.friend
            //  via core API...
            try {
                ptx.getTransaction().writeSimpleField(bar1.getObjId(), "friend", f1, false);
                assert false : "expected IllegalArgumentException";
            } catch (IllegalArgumentException e) {
                this.log.info("got expected exception: {}", e.toString());
            }
            //  via JDB API...
            try {
                ptx.writeSimpleField(bar1, "friend", foo1, false);
                assert false : "expected IllegalArgumentException";
            } catch (IllegalArgumentException e) {
                this.log.info("got expected exception: {}", e.toString());
            }

            // Try to upgrade Foo
            try {
                foo1.migrateSchema();
                assert false : "expected TypeNotInSchemaException";
            } catch (TypeNotInSchemaException e) {
                // expected
            }

            // Verify bar3 still contains references to foo's
            final PermazenObject ref1 = (PermazenObject)ptx.readSimpleField(bar3.getObjId(),
              "friend", false);                                                                         // ref1 = bar3.getFriend()
            Assert.assertTrue(ref1 instanceof UntypedPermazenObject);
            Assert.assertEquals(ref1.getObjId(), f2);
            final HashSet<ObjId> ids = ptx.readSetField(bar3.getObjId(), "set", false).stream()
              .map(obj -> ((PermazenObject)obj).getObjId())
              .collect(Collectors.toCollection(HashSet::new));
            TestSupport.checkSet(ids, buildSet(f1, f2, b1, b2));

            // Upgrade bar3 without doing any migration
            Assert.assertEquals(bar3.getSchemaId(), schemaId1);
            bar3.migrateSchema();
            Assert.assertEquals(bar3.getSchemaId(), schemaId2);

            // Now bar3 should no longer contains any references to foo's
            Assert.assertNull(ptx.readSimpleField(bar3.getObjId(), "friend", false));                    // bar3.getFriend()
            Assert.assertEquals(ref1.getObjId(), f2);
            ids.clear();
            ptx.readSetField(bar3.getObjId(), "set", false).stream()
              .map(obj -> ((PermazenObject)obj).getObjId())
              .iterator()
              .forEachRemaining(ids::add);
            TestSupport.checkSet(ids, buildSet(b1, b2));

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 20)
    public abstract static class Bar implements PermazenObject {

        @PermazenField(storageId = 21)
        public abstract Bar getFriend();
        public abstract void setFriend(Bar value);

        @PermazenField(storageId = 22)
        public abstract boolean isFix();
        public abstract void setFix(boolean fix);

        @PermazenSetField(storageId = 23, element = @PermazenField(storageId = 24))
        public abstract Set<Bar> getSet();

        @OnSchemaChange
        private void versionChange(Map<String, Object> oldValues, SchemaId oldSchemaId, SchemaId newSchemaId) {
            if (!this.isFix())
                return;
            final PermazenTransaction ptx = PermazenTransaction.getCurrent();

            // Fixup this.friend = oldFriend.getBar()
            final PermazenObject oldFriend = (PermazenObject)oldValues.get("friend");
            final ObjId barId = (ObjId)ptx.getTransaction().readSimpleField(oldFriend.getObjId(), "bar", false);
            final Bar newFriend = ptx.get(barId, Bar.class);
            this.setFriend(newFriend);
        }

        @Override
        public String toString() {
            return "Bar@" + this.getObjId();
        }

        public void dummy() {
        }
    }
}
