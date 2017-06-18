
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.function.Consumer;

import org.jsimpledb.Counter;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.annotation.JCompositeIndex;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JMapField;
import org.jsimpledb.annotation.JSetField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.DeletedObjectException;
import org.jsimpledb.core.Layout;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ReferencedObjectException;
import org.jsimpledb.index.Index;
import org.jsimpledb.index.Index2;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.test.KVTestSupport;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.tuple.Tuple2;
import org.jsimpledb.tuple.Tuple3;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;
import org.slf4j.event.Level;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JsckTest extends KVTestSupport {

    private static final int SNAPSHOT_VERSION = 1;

    private JSimpleDB jdb;

    @BeforeClass
    private void setup() {
        this.jdb = new JSimpleDB(Person.class, Pet.class);
    }

    @Test(dataProvider = "cases")
    public void testJsck(int index, JsckConfig config, NavigableMapKVStore actual, NavigableMapKVStore expected,
      Iterable<? extends Consumer<? super KVStore>> mutations) {
        this.mutateAndCompare(config, true, actual, expected, mutations);
    }

    @Test
    public void testRepairIndexes() throws Exception {

        // Setup db
        final NavigableMapKVStore actual = this.populate();
        final NavigableMapKVStore expected = actual.clone();

        // Remove all index information
        final ArrayList<Consumer<? super KVStore>> mutations = new ArrayList<>();
        for (int indexStorageId : new int[] { 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xfe, 0xef })
            actual.removeRange(KeyRange.forPrefix(UnsignedIntEncoder.encode(indexStorageId)));
        actual.removeRange(KeyRange.forPrefix(Layout.getObjectVersionIndexKeyPrefix()));

        // Test not repairing
        this.mutateAndCompare(this.getConfig(false), false, actual, actual.clone(), Collections.emptySet());

        // Test repairing
        this.mutateAndCompare(this.getConfig(true), true, actual, expected, Collections.emptySet());
    }

    private void mutateAndCompare(JsckConfig config, boolean repair, NavigableMapKVStore actual, NavigableMapKVStore expected,
      Iterable<? extends Consumer<? super KVStore>> mutations) {

        // Apply mutations
        for (Consumer<? super KVStore> mutation : mutations)
            mutation.accept(actual);

        // Run checker
        final Jsck jsck = new Jsck(config);
        final long count = jsck.check(actual, issue -> {
            log.info(String.format("JSCK: %s", issue));
            if (repair)
                issue.apply(actual);
        });

        // Compare result
        final String actualXml = this.toXmlString(actual);
        final String expectedXml = this.toXmlString(expected);
        final String diff = this.diff(expectedXml, actualXml);
        assert diff == null : "Difference in resulting XML:\n" + diff;
    }

    private NavigableMapKVStore populate() {
        final NavigableMapKVStore kv = new NavigableMapKVStore();
        this.populate(kv);
        return kv;
    }

    @DataProvider(name = "cases")
    public Object[][] genCases() {

        // Populate starting db
        final NavigableMapKVStore base = this.populate();
        //this.showKV(base, "BASE CONTENT");

        // Setup
        final JsckConfig standardConfig = this.getConfig(true);
        int index = 0;

        // Test empty database
        final ArrayList<Object[]> list = new ArrayList<>();
        list.add(new Object[] { index++, standardConfig, base.clone(), base.clone(), Collections.<Void>emptySet() });

        // Test adding random extra keys that don't belong
        final ByteWriter versionPrefixWriter = new ByteWriter();
        versionPrefixWriter.write(Layout.getObjectVersionIndexKeyPrefix());
        UnsignedIntEncoder.write(versionPrefixWriter, SNAPSHOT_VERSION);
        final byte[] versionPrefix = versionPrefixWriter.getBytes();
        for (int i = 0; i < 1; i++) {
            final NavigableMapKVStore before = base.clone();
            final NavigableMapKVStore after = base.clone();
            final ArrayList<Consumer<KVStore>> mutations = new ArrayList<>(40);
            for (int j = 0; j < 40; j++) {
                final byte[] key = this.randomBytes(0, 20, false);

                // Avoid keys starting with 0xff
                if (key.length > 0 && (key[0] & 0xff) == 0xff)
                    continue;

                // Avoid keys that intersect with meta-data
                if (Arrays.equals(key, Layout.getFormatVersionKey()))
                    continue;
                if (Arrays.equals(key, Layout.buildSchemaKey(SNAPSHOT_VERSION)))
                    continue;
                if (ByteUtil.isPrefixOf(versionPrefix, key) && key.length == versionPrefix.length + ObjId.NUM_BYTES)
                    continue;
                final byte[] value = this.randomBytes(0, 20, false);

                // Avoid keys that overwrite any existing data
                if (before.get(key) != null)
                    continue;

                // Avoid keys that could alter add/change object fields
                if (key.length > ObjId.NUM_BYTES && before.get(Arrays.copyOfRange(key, 0, ObjId.NUM_BYTES)) != null)
                    continue;

                // Add key
                mutations.add(kv -> kv.put(key, value));
            }
            list.add(new Object[] { index++, standardConfig, before, after, mutations });
        }

        // Done
        return list.toArray(new Object[list.size()][]);
    }

    private void copy(NavigableMapKVStore from, NavigableMapKVStore to) {
        to.getNavigableMap().clear();
        to.getNavigableMap().putAll(from.getNavigableMap());
    }

    private JsckConfig getConfig(boolean repair) {
        final JsckConfig config = new JsckConfig();
        config.setJsckLogger(JsckLogger.wrap(this.log, Level.INFO, Level.DEBUG));
        config.setRepair(repair);
        return config;
    }

    private void populate(NavigableMapKVStore kv) {

        final JTransaction jtx = this.jdb.createSnapshotTransaction(kv, true, ValidationMode.AUTOMATIC);

        // Create objects with known ID's we can easily recognize
        final Person mom =      this.create(jtx, Person.class, 0x1011111111111111L);
        final Person dad =      this.create(jtx, Person.class, 0x1022222222222222L);
        final Person timmy =    this.create(jtx, Person.class, 0x1033333333333333L);
        final Person beth =     this.create(jtx, Person.class, 0x1044444444444444L);
        final Person uncleBob = this.create(jtx, Person.class, 0x1055555555555555L);
        final Person auntSue =  this.create(jtx, Person.class, 0x1066666666666666L);
        final Pet spot =        this.create(jtx, Pet.class,    0x2077777777777777L);
        final Pet nemo =        this.create(jtx, Pet.class,    0x2088888888888888L);

        mom.setName("Mother");
        mom.setDOB(new Date());
        mom.setSpouse(dad);
        mom.getFriends().add(timmy);
        mom.getFriends().addAll(Arrays.asList(dad, timmy, beth, auntSue));

        dad.setName("Father");
        dad.setDOB(new Date(1394713633000L));
        dad.setSpouse(mom);
        dad.getFriends().addAll(Arrays.asList(mom, timmy, beth, uncleBob, spot));
        dad.getAmountOwed().put(timmy, 20f);
        dad.getAmountOwed().put(uncleBob, 123.45f);

        timmy.setName("Timmy");
        timmy.setDOB(new Date(1484713633000L));
        timmy.getFriends().addAll(Arrays.asList(mom, dad, uncleBob, auntSue, spot));
        dad.getAmountOwed().put(beth, 5f);

        beth.setName("Beth");
        beth.setDOB(new Date(1485713633000L));
        beth.getFriends().addAll(Arrays.asList(mom, dad, timmy, uncleBob, auntSue, spot, nemo));

        uncleBob.setName("Uncle Bob");
        uncleBob.setDOB(new Date(1394813633000L));
        uncleBob.setSpouse(auntSue);
        auntSue.setSpouse(uncleBob);

        auntSue.setName("Aunt Sue");
        auntSue.setDOB(new Date(1394814633000L));
        dad.getAmountOwed().put(uncleBob, 300.0f);

        spot.setName("Spot");
        spot.setDOB(new Date(1493713633000L));
        spot.setPetType(PetType.DOG);
        spot.setOwner(timmy);
        spot.getRaceWins().set(23);

        nemo.setName("Nemo");
        nemo.setDOB(new Date(1494713633000L));
        nemo.setPetType(PetType.GOLDFISH);
        nemo.setOwner(beth);
        spot.getRaceWins().set(0);
    }

    private <T> T create(JTransaction jtx, Class<T> type, long id) {
        final JObject jobj = jtx.get(new ObjId(id));
        jobj.recreate();
        return type.cast(jobj);
    }

// Model Classes

    public enum PetType {
        DOG,
        CAT,
        GOLDFISH;
    }

    public interface Mammal extends JObject {

        @JField(storageId = 0x99)
        Date getDOB();
        void setDOB(Date date);
    }

    public interface HasName {

        @JField(storageId = 0xaa, indexed = true)
        String getName();
        void setName(String name);
    }

    @JSimpleClass(storageId = 0x10,
      compositeIndexes = @JCompositeIndex(storageId = 0xef, name = "compidx", fields = { "DOB", "name" }))
    public interface Person extends JObject, HasName, Mammal {

        @JField(storageId = 0xbb)
        Person getSpouse();
        void setSpouse(Person spouse);

        @JListField(storageId = 0xcc, element = @JField(storageId = 0xdd, indexed = true))
        List<Mammal> getFriends();

        @JMapField(storageId = 0x34343434, key = @JField(storageId = 0x56565656, indexed = true))
        NavigableMap<Person, Float> getAmountOwed();
    }

    @JSimpleClass(storageId = 0x20)
    public interface Pet extends JObject, HasName, Mammal {

        @JField(storageId = 0xee)
        PetType getPetType();
        void setPetType(PetType type);

        @JField(storageId = 0xff)
        Counter getRaceWins();

        @JField(storageId = 0xfe)
        Person getOwner();
        void setOwner(Person owner);
    }
}
