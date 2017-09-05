
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import io.permazen.annotation.JField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.JSimpleClass;
import io.permazen.core.Database;
import io.permazen.index.Index;
import io.permazen.index.Index2;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.test.TestSupport;
import org.testng.annotations.Test;

public class TypeSafetyTest2 extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testTypeSafety2() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

    // Version 1

        final JSimpleDB jdb1 = new JSimpleDB(db, 1, null, Arrays.<Class<?>>asList(Inventory1.class, Car.class, Boat.class));
        JTransaction jtx = jdb1.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);

        final Car car;
        final Boat boat;
        final Inventory1 inventory1;

        try {

            // Create objects
            car = jtx.create(Car.class);
            boat = jtx.create(Boat.class);
            inventory1 = jtx.create(Inventory1.class);

            inventory1.getVehicleMap().put(car, Color.RED);
            inventory1.getVehicleMap().put(boat, Color.BLUE);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

    // Version 2

        final JSimpleDB jdb2 = new JSimpleDB(db, 2, null, Arrays.<Class<?>>asList(Inventory2.class, Car.class, Boat.class));
        jtx = jdb2.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);

        final Inventory2 inventory2;

        try {

            inventory2 = jtx.getAll(Inventory2.class).iterator().next();

            for (Map.Entry<Color, Index<Inventory2, Car>> entry : Inventory2.queryColorIndex().asMapOfIndex().entrySet()) {
                final Color color = entry.getKey();
                final NavigableMap<Inventory2, NavigableSet<Car>> map = entry.getValue().asMap();
                assert map.size() == 1;
                for (Car car2 : map.values().iterator().next())
                    this.log.info("found map value index entry with color " + color + " and car " + car2);
            }

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    public enum Color {
        RED,
        BLUE;
    };

    @JSimpleClass(storageId = 20)
    public abstract static class Vehicle implements JObject {

        @Override
        public String toString() {
            return this.getClass().getSimpleName().replaceAll("\\$\\$JSimpleDB$", "") + "@" + this.getObjId();
        }
    }

    @JSimpleClass(storageId = 30)
    public abstract static class Car extends Vehicle {
    }

    @JSimpleClass(storageId = 40)
    public abstract static class Boat extends Vehicle {
    }

    // Version 1
    @JSimpleClass(storageId = 50)
    public abstract static class Inventory1 extends Vehicle {

        @JMapField(storageId = 51, key = @JField(storageId = 52), value = @JField(storageId = 53, indexed = true))
        public abstract NavigableMap<Vehicle, Color> getVehicleMap();
    }

    // Version 2
    @JSimpleClass(storageId = 50)
    public abstract static class Inventory2 extends Vehicle {

        @JMapField(storageId = 51, key = @JField(storageId = 52), value = @JField(storageId = 53, indexed = true))
        public abstract NavigableMap<Car, Color> getCarMap();         // note: key restricted to "Car" from "Vehicle"

        public static Index2<Color, Inventory2, Car> queryColorIndex() {
            return JTransaction.getCurrent().queryMapValueIndex(Inventory2.class, "carMap.value", Color.class, Car.class);
        }
    }
}

