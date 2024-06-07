
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenTransient;
import io.permazen.annotation.PermazenType;
import io.permazen.core.ObjId;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ImportExportTest extends MainTestSupport {

    @Test
    public void testImportExport() {

        // Create POJO objects
        Person dad1 = new Person("Dad");
        Person mom1 = new Person("Mom");
        Person margo1 = new Person("Margaret");

        this.initializePeople(dad1, mom1, margo1);

        // Do import
        final ObjId dadId;
        final ObjId momId;
        final ObjId margoId;

        final Permazen pdb = BasicTest.newPermazen(Person.class);
        PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            // Do import
            final ImportContext importContext = new ImportContext(ptx);
            final Person dad2 = (Person)importContext.importPlain(dad1);
            final Person margo2 = (Person)importContext.importPlain(margo1);
            final Person mom2 = ptx.get(importContext.getObjectMap().get(mom1), Person.class);

            Assert.assertTrue(((PermazenObject)dad2).exists());
            Assert.assertTrue(((PermazenObject)mom2).exists());
            Assert.assertTrue(((PermazenObject)margo2).exists());

            this.checkPeople(dad2, mom2, margo2);

            // Save ID's for next transaction
            dadId = ((PermazenObject)dad2).getObjId();
            momId = ((PermazenObject)mom2).getObjId();
            margoId = ((PermazenObject)margo2).getObjId();

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        // Do export
        Person dad3;
        Person mom3;
        Person margo3;

        ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Person dad2 = ptx.get(dadId, Person.class);
            final Person mom2 = ptx.get(momId, Person.class);
            final Person margo2 = ptx.get(margoId, Person.class);

            // Do export
            final ExportContext exportContext = new ExportContext(ptx);
            dad3 = (Person)exportContext.exportPlain((PermazenObject)dad2);
            mom3 = (Person)exportContext.exportPlain((PermazenObject)mom2);
            margo3 = (Person)exportContext.getPermazenObjectMap().get(margoId);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }

        // Verify export
        this.checkPeople(dad3, mom3, margo3);
    }

    private void initializePeople(Person dad, Person mom, Person margo) {

        dad.setSpouse(mom);
        dad.setBirthday(LocalDate.parse("1980-03-05"));
        dad.setColorPrefs(new EnumMap<>(Color.class));
        dad.getColorPrefs().put(Color.RED, 7.5f);

        mom.setSpouse(dad);
        mom.setBirthday(LocalDate.parse("1982-11-16"));
        mom.setColorPrefs(new HashMap<>());
        mom.getColorPrefs().put(Color.GREEN, 8.0f);
        mom.getColorPrefs().put(Color.BLUE, 5.0f);
        mom.setFriends(new HashSet<>());
        mom.getFriends().add(margo);

        margo.setNicknames(new ArrayList<>());
        margo.getNicknames().add("Margo");
        margo.getNicknames().add("Peggy");
        margo.setFriends(new HashSet<>());
        margo.getFriends().add(dad);
        margo.getFriends().add(mom);
    }

    private void checkPeople(Person dad, Person mom, Person margo) {

        // Check Dad
        Assert.assertNotNull(dad);
        Assert.assertEquals(dad.getName(), "Dad");
        Assert.assertSame(dad.getSpouse(), mom);
        Assert.assertEquals(dad.getBirthday(), LocalDate.parse("1980-03-05"));
        Assert.assertEquals(dad.getColorPrefs(), Collections.singletonMap(Color.RED, 7.5f));
        Assert.assertEquals(dad.getNicknames().size(), 0);

        // Check Mom
        Assert.assertNotNull(mom);
        Assert.assertEquals(mom.getName(), "Mom");
        Assert.assertSame(mom.getSpouse(), dad);
        Assert.assertEquals(mom.getBirthday(), LocalDate.parse("1982-11-16"));
        Assert.assertEquals(mom.getColorPrefs().get(Color.RED), null);
        Assert.assertEquals((float)mom.getColorPrefs().get(Color.GREEN), 8.0f);
        Assert.assertEquals((float)mom.getColorPrefs().get(Color.BLUE), 5.0f);
        Assert.assertEquals(mom.getFriends().size(), 1);
        Assert.assertTrue(mom.getFriends().contains(margo));
        Assert.assertEquals(mom.getNicknames().size(), 0);

        // Check Margo
        Assert.assertNotNull(margo);
        Assert.assertEquals(margo.getName(), "Margaret");
        Assert.assertEquals(margo.getNicknames(), Arrays.asList("Margo", "Peggy"));
        Assert.assertEquals(margo.getFriends().size(), 2);
        Assert.assertTrue(margo.getFriends().contains(dad));
        Assert.assertTrue(margo.getFriends().contains(mom));
    }

// Model Classes

    public enum Color {
        RED,
        GREEN,
        BLUE;
    }

    @PermazenType(autogenNonAbstract = true)
    public static class Person {

        private String name;
        private List<String> nicknames;
        private Person spouse;
        private Map<Color, Float> colorPrefs;
        private LocalDate birthday;
        private Set<Person> friends;
        private int dummy;

        public Person() {
        }

        @SuppressWarnings("this-escape")
        public Person(String name) {
            this.setName(name);
        }

        public String getName() {
            return this.name;
        }
        public void setName(String name) {
            this.name = name;
        }

        public List<String> getNicknames() {
            return this.nicknames;
        }
        public void setNicknames(List<String> nicknames) {
            this.nicknames = nicknames;
        }

        public Person getSpouse() {
            return this.spouse;
        }
        public void setSpouse(Person spouse) {
            this.spouse = spouse;
        }

        public Map<Color, Float> getColorPrefs() {
            return this.colorPrefs;
        }
        public void setColorPrefs(Map<Color, Float> colorPrefs) {
            this.colorPrefs = colorPrefs;
        }

        public LocalDate getBirthday() {
            return this.birthday;
        }
        public void setBirthday(LocalDate birthday) {
            this.birthday = birthday;
        }

        public Set<Person> getFriends() {
            return this.friends;
        }
        public void setFriends(Set<Person> friends) {
            this.friends = friends;
        }

        @PermazenTransient
        public int getDummy() {
            return this.dummy;
        }
        public void setDummy(int dummy) {
            this.dummy = dummy;
        }

        @Override
        public String toString() {
            return "Person[" + this.getName() + "]";
        }
    }
}
