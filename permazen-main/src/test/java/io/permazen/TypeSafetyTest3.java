
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JSetField;
import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenType;
import io.permazen.change.SetFieldRemove;
import io.permazen.change.SimpleFieldChange;
import io.permazen.core.Database;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.test.TestSupport;

import java.util.Arrays;
import java.util.NavigableSet;

import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class TypeSafetyTest3 extends TestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testTypeSafety3() throws Exception {

        final SimpleKVDatabase kvstore = new SimpleKVDatabase();
        final Database db = new Database(kvstore);

    // Version 1

        final Permazen jdb1 = new Permazen(db, 1, null, Arrays.<Class<?>>asList(Inventory1.class, Car.class, Boat.class));
        JTransaction jtx = jdb1.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);

        Car car;
        Boat boat;
        Inventory1 inventory1;

        try {

            // Create objects
            car = jtx.create(Car.class);
            car.setColor(Color.RED);
            boat = jtx.create(Boat.class);
            boat.setColor(Color.BLUE);
            inventory1 = jtx.create(Inventory1.class);

            inventory1.getVehicles().add(car);
            inventory1.getVehicles().add(boat);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

    // Version 2

        final Permazen jdb2 = new Permazen(db, 2, null, Arrays.<Class<?>>asList(Inventory2.class, Car.class, Boat.class));
        jtx = jdb2.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);

        final Inventory2 inventory2;

        try {

            inventory2 = jtx.getAll(Inventory2.class).iterator().next();

            // Reload objects
            car = jtx.get(car);
            boat = jtx.get(boat);

            boat.setColor(Color.RED);   // triggers notification to carColorChange()?

            inventory2.getCars();       // forces version upgrade, which removes boat, triggering notification to carRemoved()?

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

    @PermazenType(storageId = 20)
    public abstract static class Vehicle implements JObject {

        @JField(storageId = 21)
        public abstract Color getColor();
        public abstract void setColor(Color color);

        @Override
        public String toString() {
            return this.getClass().getSimpleName().replaceAll("\\$\\$Permazen$", "") + "@" + this.getObjId();
        }
    }

    @PermazenType(storageId = 30)
    public abstract static class Car extends Vehicle {
    }

    @PermazenType(storageId = 40)
    public abstract static class Boat extends Vehicle {
    }

    // Version 1
    @PermazenType(storageId = 50)
    public abstract static class Inventory1 extends Vehicle {

        @JSetField(storageId = 51, element = @JField(storageId = 52))
        public abstract NavigableSet<Vehicle> getVehicles();
    }

    // Version 2
    @PermazenType(storageId = 50)
    public abstract static class Inventory2 extends Vehicle {

        @JSetField(storageId = 51, element = @JField(storageId = 52))
        public abstract NavigableSet<Car> getCars();

        @OnChange("cars.element.color")
        private void carColorChange(SimpleFieldChange<Car, Color> change) {
            final Car car = change.getObject();
            final Color oldColor = change.getOldValue();
            final Color newColor = change.getNewValue();
            LoggerFactory.getLogger(Inventory2.class).info("car " + car + " color changed from " + oldColor + " -> " + newColor);
        }

        @OnChange("cars")
        private void carRemoved(SetFieldRemove<Inventory2, Car> change) {
            final Car car = change.getElement();
            LoggerFactory.getLogger(Inventory2.class).info("car " + car + " removed from " + this);
        }
    }
}

