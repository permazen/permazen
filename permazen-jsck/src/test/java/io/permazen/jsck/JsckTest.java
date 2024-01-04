
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import io.permazen.Counter;
import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.Permazen;
import io.permazen.PermazenConfig;
import io.permazen.ValidationMode;
import io.permazen.annotation.JCompositeIndex;
import io.permazen.annotation.JField;
import io.permazen.annotation.JListField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.Encodings;
import io.permazen.core.Layout;
import io.permazen.core.ObjId;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRange;
import io.permazen.kv.test.KVTestSupport;
import io.permazen.kv.util.MemoryKVStore;
import io.permazen.util.ByteUtil;
import io.permazen.util.ByteWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.function.Consumer;

import org.slf4j.event.Level;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JsckTest extends KVTestSupport {

    private static final int SNAPSHOT_SCHEMA_INDEX = 1;

    private Permazen jdb;

    @BeforeClass
    private void setupTestDatabase() {
        this.jdb = PermazenConfig.builder()
          .modelClasses(Person.class, Pet.class)
          .build()
          .newPermazen();
    }

    @Test(dataProvider = "cases")
    public void testJsck(int index, JsckConfig config, MemoryKVStore actual, MemoryKVStore expected,
      Iterable<? extends Consumer<? super KVStore>> mutations) {
        this.mutateAndCompare(config, true, actual, expected, mutations);
    }

    @Test
    public void testRepairIndexes() throws Exception {

        // Setup db
        final MemoryKVStore actual = this.populate();
        final MemoryKVStore expected = actual.clone();

        // Remove all index information
        final ArrayList<Consumer<? super KVStore>> mutations = new ArrayList<>();
        for (int indexStorageId : new int[] { 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0xfe, 0xef })
            actual.removeRange(KeyRange.forPrefix(Encodings.UNSIGNED_INT.encode(indexStorageId)));
        actual.removeRange(KeyRange.forPrefix(Layout.getSchemaIndexKeyPrefix()));

        // Test not repairing
        this.mutateAndCompare(this.getConfig(false), false, actual, actual.clone(), Collections.emptySet());

        // Test repairing
        this.mutateAndCompare(this.getConfig(true), true, actual, expected, Collections.emptySet());
    }

    @Test
    public void testDeletedReference() throws Exception {

        // Setup db
        final MemoryKVStore kv = new MemoryKVStore();
        final JTransaction jtx = this.jdb.createDetachedTransaction(kv, ValidationMode.AUTOMATIC);

        // Create objects with known ID's we can easily recognize
        final Person p1 = this.create(jtx, Person.class, 0x1011111111111111L);
        final Person deleted = this.create(jtx, Person.class, 0x1022222222222222L);
        final Person p3 = this.create(jtx, Person.class, 0x1033333333333333L);

        // Set up refs
        p1.setSpouse(deleted);
        p1.getAmountOwed().put(p1, 545.45f);
        p1.getAmountOwed().put(deleted, 123.45f);
        p1.getAmountOwed().put(p3, 616.34f);
        p3.getFriends().add(p1);
        p3.getFriends().add(deleted);
        p3.getFriends().add(p3);
        p3.getFriends().add(p1);

        // Secretly delete p3
        final ObjId deletedId = deleted.getObjId();
        final int deletedSchemaIndex = jtx.getTransaction().getObjType(deletedId).getSchema().getSchemaIndex();
        kv.removeRange(KeyRange.forPrefix(deletedId.getBytes()));
        kv.remove(ByteUtil.parse("aaff1022222222222222"));                  // index entry for delete.name
        kv.remove(ByteUtil.parse("bbff1022222222222222"));                  // index entry for delete.spouse
        kv.remove(Layout.buildSchemaIndexKey(deletedId, deletedSchemaIndex));

        // Prepare KV's
        final MemoryKVStore damaged = kv.clone();
        p1.setSpouse(null);
        p1.getAmountOwed().remove(deleted);
        p3.getFriends().remove(1);
        final MemoryKVStore repaired = kv.clone();

        // Test repair - dangling references to deleted should get cleaned up
        this.repairAndCompare(this.getConfig(true), damaged, repaired);
    }

    @Test
    public void testSchemaCompat() throws Exception {
        final MemoryKVStore kv = new MemoryKVStore();
        final Permazen refsDB = PermazenConfig.builder()
          .modelClasses(Refs1.class, Refs2.class, Person.class, Pet.class)
          .build()
          .newPermazen();
        final JTransaction jtx = refsDB.createDetachedTransaction(kv, ValidationMode.AUTOMATIC);

        final Person person1 = this.create(jtx, Person.class, 0x1011111111111111L);
        final Person person2 = this.create(jtx, Person.class, 0x1022222222222222L);
        final Pet pet1 = this.create(jtx, Pet.class, 0x2033333333333333L);
        final Pet pet2 = this.create(jtx, Pet.class, 0x2044444444444444L);

        final Refs1 refs1 = this.create(jtx, Refs1.class, 0x5055555555555555L);
        refs1.setRef(person1);
        refs1.getList().add(person2);
        refs1.getSet().add(person1);
        refs1.getMap1().put(person2, pet1);
        refs1.getMap2().put(pet2, person1);

        final Refs2 refs2 = this.create(jtx, Refs2.class, 0x6066666666666666L);
        refs2.setRef(pet1);
        refs2.getList().add(pet2);
        refs2.getSet().add(pet1);
        refs2.getMap1().put(pet2, person1);
        refs2.getMap2().put(person2, pet1);

        this.repairAndCompare(this.getConfig(true), kv.clone(), kv.clone());
    }

    private void mutateAndCompare(JsckConfig config, boolean repair, MemoryKVStore actual, MemoryKVStore expected,
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

    private void repairAndCompare(JsckConfig config, MemoryKVStore damaged, MemoryKVStore repaired) {

        // Run checker
        final Jsck jsck = new Jsck(config);
        final long count = jsck.check(damaged, issue -> {
            log.info(String.format("JSCK: %s", issue));
            issue.apply(damaged);
        });

        // Compare result
        final String actualXml = this.toXmlString(damaged);
        final String expectedXml = this.toXmlString(repaired);
        final String diff = this.diff(expectedXml, actualXml);
        assert diff == null : "Difference in resulting XML:\n" + diff;
    }

    private MemoryKVStore populate() {
        final MemoryKVStore kv = new MemoryKVStore();
        this.populate(kv);
        return kv;
    }

    @DataProvider(name = "cases")
    public Object[][] genCases() {

        // Populate starting db
        final MemoryKVStore base = this.populate();
        //this.showKV(base, "BASE CONTENT");

        // Setup
        final JsckConfig standardConfig = this.getConfig(true);
        int index = 0;

        // Test empty database
        final ArrayList<Object[]> list = new ArrayList<>();
        list.add(new Object[] { index++, standardConfig, base.clone(), base.clone(), Collections.<Void>emptySet() });

        // Get key prefixes
        final byte[] formatVersionKey = Layout.getFormatVersionKey();
        final byte[] schemaTablePrefix = Layout.getSchemaTablePrefix();
        final byte[] storageIdTablePrefix = Layout.getStorageIdTablePrefix();
        final byte[] schemaIndexKeyPrefix = Layout.getSchemaIndexKeyPrefix();

        // Test adding random extra keys that don't belong
        final ByteWriter versionPrefixWriter = new ByteWriter();
        versionPrefixWriter.write(schemaIndexKeyPrefix);
        Encodings.UNSIGNED_INT.write(versionPrefixWriter, SNAPSHOT_SCHEMA_INDEX);
        final byte[] versionPrefix = versionPrefixWriter.getBytes();
        for (int i = 0; i < 1; i++) {
            final MemoryKVStore before = base.clone();
            final MemoryKVStore after = base.clone();
            final ArrayList<Consumer<KVStore>> mutations = new ArrayList<>(40);
            for (int j = 0; j < 40; j++) {
                final byte[] key = this.randomBytes(0, 20, false);

                // Avoid keys starting with 0xff
                if (key.length > 0 && (key[0] & 0xff) == 0xff)
                    continue;

                // Avoid keys that intersect with meta-data
                if (Arrays.equals(key, formatVersionKey))
                    continue;
                if (ByteUtil.isPrefixOf(schemaTablePrefix, key))
                    continue;
                if (ByteUtil.isPrefixOf(storageIdTablePrefix, key))
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

    private void copy(MemoryKVStore from, MemoryKVStore to) {
        to.getNavigableMap().clear();
        to.getNavigableMap().putAll(from.getNavigableMap());
    }

    private JsckConfig getConfig(boolean repair) {
        final JsckConfig config = new JsckConfig();
        config.setJsckLogger(JsckLogger.wrap(this.log, Level.INFO, Level.DEBUG));
        config.setRepair(repair);
        return config;
    }

    private void populate(MemoryKVStore kv) {

        final JTransaction jtx = this.jdb.createDetachedTransaction(kv, ValidationMode.AUTOMATIC);

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

    @JCompositeIndex(storageId = 0xef, name = "compidx", fields = { "DOB", "name" })
    @PermazenType(storageId = 0x10)
    public interface Person extends JObject, HasName, Mammal {

        @JField(storageId = 0xbb)
        Person getSpouse();
        void setSpouse(Person spouse);

        @JListField(storageId = 0xcc, element = @JField(storageId = 0xdd, indexed = true))
        List<Mammal> getFriends();

        @JMapField(storageId = 0x34343434, key = @JField(storageId = 0x56565656, indexed = true))
        NavigableMap<Person, Float> getAmountOwed();
    }

    @PermazenType(storageId = 0x20)
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

    public interface Refs<T1 extends JObject, T2 extends JObject> extends JObject {
        T1 getRef();
        void setRef(T1 x);
        List<T1> getList();
        NavigableSet<T1> getSet();
        NavigableMap<T1, T2> getMap1();
        NavigableMap<T2, T1> getMap2();
    }

    @PermazenType(storageId = 0x50)
    public abstract static class Refs1 implements Refs<Person, Pet> {
    }

    @PermazenType(storageId = 0x60)
    public abstract static class Refs2 implements Refs<Pet, Person> {
    }
}
