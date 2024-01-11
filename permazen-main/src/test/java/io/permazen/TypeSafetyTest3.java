
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.OnChange;
import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.change.SetFieldRemove;
import io.permazen.change.SimpleFieldChange;
import io.permazen.core.Database;
import io.permazen.kv.simple.MemoryKVDatabase;

import java.util.NavigableSet;

import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class TypeSafetyTest3 extends MainTestSupport {

    @SuppressWarnings("unchecked")
    @Test
    public void testTypeSafety3() throws Exception {

        final Database db = new Database(new MemoryKVDatabase());

    // Version 1

        final Permazen jdb1 = BasicTest.newPermazen(db, Inventory1.class, Car.class, Boat.class);
        PermazenTransaction ptx = jdb1.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);

        Car car;
        Boat boat;
        Inventory1 inventory1;

        try {

            // Create objects
            car = ptx.create(Car.class);
            car.setColor(Color.RED);
            boat = ptx.create(Boat.class);
            boat.setColor(Color.BLUE);
            inventory1 = ptx.create(Inventory1.class);

            inventory1.getVehicles().add(car);
            inventory1.getVehicles().add(boat);

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }

    // Version 2

        final Permazen jdb2 = BasicTest.newPermazen(db, Inventory2.class, Car.class, Boat.class);
        ptx = jdb2.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);

        final Inventory2 inventory2;

        try {

            inventory2 = ptx.getAll(Inventory2.class).iterator().next();

            // Reload objects
            car = ptx.get(car);
            boat = ptx.get(boat);

            boat.setColor(Color.RED);   // triggers notification to carColorChange()?

            inventory2.getCars();       // forces version upgrade, which removes boat, triggering notification to carRemoved()?

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    public enum Color {
        RED,
        BLUE;
    };

    @PermazenType(storageId = 20)
    public abstract static class Vehicle implements PermazenObject {

        @PermazenField(storageId = 21)
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

        @PermazenSetField(storageId = 51, element = @PermazenField(storageId = 52))
        public abstract NavigableSet<Vehicle> getVehicles();
    }

    // Version 2
    @PermazenType(storageId = 50)
    public abstract static class Inventory2 extends Vehicle {

        @PermazenSetField(storageId = 51, element = @PermazenField(storageId = 52))
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
