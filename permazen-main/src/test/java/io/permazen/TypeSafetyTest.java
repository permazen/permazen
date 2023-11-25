
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JSetField;
import io.permazen.annotation.OnVersionChange;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.TransactionConfig;
import io.permazen.core.TypeNotInSchemaVersionException;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;
import io.permazen.util.Streams;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TypeSafetyTest extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testTypeSafety() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

    // Version 1

        final SchemaModel schema1 = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema>\n"
          + "  <ObjectType name=\"Foo\" storageId=\"10\">\n"
          + "    <SimpleField name=\"ival\" encoding=\"urn:fdc:permazen.io:2020:int\" storageId=\"11\"/>\n"
          + "    <ReferenceField name=\"bar\" storageId=\"12\">\n"
          + "      <ObjectTypes>\n"
          + "        <ObjectType storageId=\"20\"/>\n"
          + "      </ObjectTypes>\n"
          + "    </ReferenceField>\n"
          + "  </ObjectType>\n"
          + "  <ObjectType name=\"Bar\" storageId=\"20\">\n"
          + "    <ReferenceField name=\"friend\" storageId=\"21\"/>\n"
          + "    <SimpleField name=\"fix\" encoding=\"urn:fdc:permazen.io:2020:boolean\" storageId=\"22\"/>\n"
          + "    <SetField name=\"set\" storageId=\"23\">\n"
          + "      <ReferenceField storageId=\"24\">\n"
          + "        <ObjectTypes>\n"
          + "          <ObjectType storageId=\"10\"/>\n"
          + "          <ObjectType storageId=\"20\"/>\n"
          + "        </ObjectTypes>\n"
          + "      </ReferenceField>\n"
          + "    </SetField>"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes(StandardCharsets.UTF_8)));

        final Database db = new Database(kvstore);
        final TransactionConfig txConfig1 = TransactionConfig.builder()
          .schemaModel(schema1)
          .schemaVersion(1)
          .build();
        Transaction tx = db.createTransaction(txConfig1);

        final ObjId f1 = tx.create(10);
        final ObjId f2 = tx.create(10);
        final ObjId b1 = tx.create(20);
        final ObjId b2 = tx.create(20);
        final ObjId b3 = tx.create(20);

        tx.writeSimpleField(b1, 21, f1, false);                             // bar1.setFriend(foo1)
        tx.writeSimpleField(f1, 11, 1234, false);                           // foo1.setIVal(1234)
        tx.writeSimpleField(f1, 12, b2, false);                             // foo1.setBar(bar2)

        final NavigableSet<ObjId> bar3set = (NavigableSet<ObjId>)tx.readSetField(b3, 23, false);
        tx.writeSimpleField(b3, 21, f2, false);                             // bar3.setFriend(foo2)
        bar3set.add(b1);
        bar3set.add(b2);
        bar3set.add(f1);
        bar3set.add(f2);

        // Attempt to set disallowed references
        try {
            tx.writeSimpleField(f1, 12, f1, false);                         // foo1.setBar(foo1)
            assert false : "expected IllegalArgumentException";
        } catch (IllegalArgumentException e) {
            this.log.info("got expected exception: {}", e.toString());
        }
        Assert.assertEquals(tx.readSimpleField(f1, 12, false), b2);         // verify foo1.getBar() == bar2

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

        Permazen jdb = new Permazen(db, 2, null, Arrays.<Class<?>>asList(Bar.class));       // note: no Foo.class, only Bar.class
        JTransaction jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            // Get objects
            final JObject foo1 = jtx.get(f1, JObject.class);
            final JObject foo2 = jtx.get(f2, JObject.class);
            final Bar bar1 = jtx.get(b1, Bar.class);
            final Bar bar2 = jtx.get(b2, Bar.class);
            final Bar bar3 = jtx.get(b3, Bar.class);

            // Verify index on Bar.friend does not contain any type "Foo" keys
            final NavigableMap<Bar, NavigableSet<Bar>> friendIndex = jtx.queryIndex(Bar.class, "friend", Bar.class).asMap();
            Streams.iterate(friendIndex.keySet().stream().filter(key -> key != null), Bar::dummy);

            // Verify index on Bar.set.element does not contain any type "Foo" keys
            final NavigableMap<Bar, NavigableSet<Bar>> setIndex = jtx.queryIndex(Bar.class, "set.element", Bar.class).asMap();
            Streams.iterate(setIndex.keySet().stream().filter(key -> key != null), Bar::dummy);

            // Verify bar1 has wrongly type'd field prior to upgrade
            Assert.assertEquals(jtx.getTransaction().readSimpleField(b1, 21, false), f1);

            // Configure bar1 to update the field on version update
            jtx.getTransaction().writeSimpleField(bar1.getObjId(), 22, true, false);    // bar1.setFix(true)

            // Attempt to read wrongly type'd field after upgrade migration
            final Bar friend = bar1.getFriend();
            Assert.assertSame(friend, bar2);

            // Try to assign invalid Foo typed value to bar1.friend
            //  via core API...
            try {
                jtx.getTransaction().writeSimpleField(bar1.getObjId(), 21, f1, false);
                assert false : "expected IllegalArgumentException";
            } catch (IllegalArgumentException e) {
                this.log.info("got expected exception: {}", e.toString());
            }
            //  via JDB API...
            try {
                jtx.writeSimpleField(bar1, 21, foo1, false);
                assert false : "expected IllegalArgumentException";
            } catch (IllegalArgumentException e) {
                this.log.info("got expected exception: {}", e.toString());
            }

            // Try to upgrade Foo
            try {
                foo1.upgrade();
                assert false : "expected TypeNotInSchemaVersionException";
            } catch (TypeNotInSchemaVersionException e) {
                // expected
            }

            // Verify bar3 still contains references to foo's
            final JObject ref1 = (JObject)jtx.readSimpleField(bar3.getObjId(), 21, false);         // ref1 = bar3.getFriend()
            Assert.assertTrue(ref1 instanceof UntypedJObject);
            Assert.assertEquals(ref1.getObjId(), f2);
            final HashSet<ObjId> ids = jtx.readSetField(bar3.getObjId(), 23, false).stream()
              .map(obj -> ((JObject)obj).getObjId())
              .collect(Collectors.toCollection(HashSet::new));
            TestSupport.checkSet(ids, buildSet(f1, f2, b1, b2));

            // Upgrade bar3 without doing any migration
            Assert.assertEquals(bar3.getSchemaVersion(), 1);
            bar3.upgrade();
            Assert.assertEquals(bar3.getSchemaVersion(), 2);

            // Now bar3 should no longer contains any references to foo's
            Assert.assertNull(jtx.readSimpleField(bar3.getObjId(), 21, false));                    // bar3.getFriend()
            Assert.assertEquals(ref1.getObjId(), f2);
            ids.clear();
            Streams.iterate(jtx.readSetField(bar3.getObjId(), 23, false).stream()
                .map(obj -> ((JObject)obj).getObjId()),
              ids::add);
            TestSupport.checkSet(ids, buildSet(b1, b2));

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 20)
    public abstract static class Bar implements JObject {

        @JField(storageId = 21)
        public abstract Bar getFriend();
        public abstract void setFriend(Bar value);

        @JField(storageId = 22)
        public abstract boolean isFix();
        public abstract void setFix(boolean fix);

        @JSetField(storageId = 23, element = @JField(storageId = 24))
        public abstract Set<Bar> getSet();

        @OnVersionChange
        private void versionChange(int oldVersion, int newVersion, Map<Integer, Object> oldValues) {
            if (!this.isFix())
                return;
            final JTransaction jtx = JTransaction.getCurrent();

            // Fixup this.friend = oldFriend.getBar()
            final JObject oldFriend = (JObject)oldValues.get(21);
            final ObjId barId = (ObjId)jtx.getTransaction().readSimpleField(oldFriend.getObjId(), 12, false);
            final Bar newFriend = jtx.get(barId, Bar.class);
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
