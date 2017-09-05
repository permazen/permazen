
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.NavigableMap;
import java.util.SortedSet;

import io.permazen.annotation.JCompositeIndex;
import io.permazen.annotation.JField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.JSimpleClass;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.schema.SchemaModel;
import io.permazen.test.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StorageIdTest extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testStorageIds() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();

        final DefaultStorageIdGenerator storageIdGenerator = new DefaultStorageIdGenerator();
        final int fooId = storageIdGenerator.generateClassStorageId(Foo.class, "Foo");
        final int stringId = storageIdGenerator.generateFieldStorageId(Foo.class.getMethod("getString"), "string");
        final int friendId = storageIdGenerator.generateFieldStorageId(Friendly.class.getMethod("getFriend"), "friend");
        final int setId = storageIdGenerator.generateFieldStorageId(Foo.class.getMethod("getSet"), "set");
        final int setElementId = storageIdGenerator.generateSetElementStorageId(Foo.class.getMethod("getSet"), "set");
        final int listId = storageIdGenerator.generateFieldStorageId(Foo.class.getMethod("getList"), "list");
        final int listElementId = storageIdGenerator.generateListElementStorageId(Foo.class.getMethod("getList"), "list");
        final int mapId = storageIdGenerator.generateFieldStorageId(Foo.class.getMethod("getMap"), "map");
        final int mapKeyId = storageIdGenerator.generateMapKeyStorageId(Foo.class.getMethod("getMap"), "map");
        final int mapValueId = storageIdGenerator.generateMapValueStorageId(Foo.class.getMethod("getMap"), "map");
        final int indexId = storageIdGenerator.generateCompositeIndexStorageId(Foo.class,
          "index", new int[] { stringId, friendId });

        final SchemaModel expected = SchemaModel.fromXML(new ByteArrayInputStream((
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<Schema formatVersion=\"2\">\n"
          + "  <ObjectType name=\"Foo\" storageId=\"" + fooId + "\">\n"
          + "    <SimpleField name=\"string\" type=\"java.lang.String\" storageId=\"" + stringId + "\"/>\n"
          + "    <ReferenceField name=\"friend\" storageId=\"" + friendId + "\">\n"
          + "      <ObjectTypes>\n"
          + "        <ObjectType storageId=\"" + fooId + "\"/>\n"
          + "      </ObjectTypes>\n"
          + "    </ReferenceField>\n"
          + "    <SetField name=\"set\" storageId=\"" + setId + "\">\n"
          + "      <SimpleField type=\"java.lang.Integer\" storageId=\"" + setElementId + "\"/>\n"
          + "    </SetField>"
          + "    <ListField name=\"list\" storageId=\"" + listId + "\">\n"
          + "      <SimpleField type=\"java.lang.Float\" storageId=\"" + listElementId + "\"/>\n"
          + "    </ListField>"
          + "    <MapField name=\"map\" storageId=\"" + mapId + "\">\n"
          + "      <ReferenceField storageId=\"" + mapKeyId + "\">\n"
          + "        <ObjectTypes>\n"
          + "          <ObjectType storageId=\"" + fooId + "\"/>\n"
          + "        </ObjectTypes>\n"
          + "      </ReferenceField>\n"
          + "      <SimpleField type=\"double\" storageId=\"" + mapValueId + "\"/>\n"
          + "    </MapField>"
          + "    <CompositeIndex name=\"index\" storageId=\"" + indexId + "\">\n"
          + "      <IndexedField storageId=\"" + stringId + "\"/>\n"
          + "      <IndexedField storageId=\"" + friendId + "\"/>\n"
          + "    </CompositeIndex>\n"
          + "  </ObjectType>\n"
          + "</Schema>\n"
          ).getBytes("UTF-8")));

        final SchemaModel actual = new JSimpleDB(Foo.class).getSchemaModel();

        Assert.assertEquals(actual, expected);
    }

// Model Classes

    public interface Friendly {

        Foo getFriend();
        void setFriend(Foo friend);
    }

    @JCompositeIndex(name = "index", fields = { "string", "friend" })
    @JSimpleClass
    public abstract static class Foo implements JObject, Friendly {

        public abstract String getString();
        public abstract void setString(String string);

        public abstract SortedSet<Integer> getSet();

        public abstract List<Float> getList();

        @JMapField(value = @JField(type = "double"))
        public abstract NavigableMap<Friendly, Double> getMap();

        // Not a bean getter method, should be ignored
        public int someMethod(int y) {
            return y + 1;
        }

        // Not an abstract method, should be ignored
        public int getFoo() {
            return 0;
        }
        public void setFoo(int foo) {
        }

        @Override
        public String toString() {
            return "Foo@" + this.getObjId();
        }
    }
}

