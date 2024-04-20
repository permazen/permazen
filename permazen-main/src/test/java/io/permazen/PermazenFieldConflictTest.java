
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.annotation.Values;
import io.permazen.core.DeleteAction;

import org.testng.annotations.Test;

/**
 * This test checks for conflicting @PermazenField annotations inherited from different supertypes.
 */
public class PermazenFieldConflictTest extends MainTestSupport {

// Indexed conflict

    @Test
    public void testIndexedConflict() {
        try {
            BasicTest.newPermazen(IndexedConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface IndexedConflict1 {
        @PermazenField(indexed = false)
        float getRef();
    }
    public interface IndexedConflict2 {
        @PermazenField(indexed = true)
        float getRef();
    }
    @PermazenType
    public interface IndexedConflict extends IndexedConflict1, IndexedConflict2 {
        void setRef(float x);
    }

// InverseDelete conflict

    @Test
    public void testInverseDeleteConflict() {
        try {
            BasicTest.newPermazen(InverseDeleteConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface InverseDeleteConflict1 {
        @PermazenField(inverseDelete = DeleteAction.UNREFERENCE)
        InverseDeleteConflict getRef();
    }
    public interface InverseDeleteConflict2 {
        @PermazenField(inverseDelete = DeleteAction.EXCEPTION)
        InverseDeleteConflict getRef();
    }
    @PermazenType
    public interface InverseDeleteConflict extends InverseDeleteConflict1, InverseDeleteConflict2 {
        void setRef(InverseDeleteConflict x);
    }

// ForwardDelete conflict

    @Test
    public void testForwardDeleteConflict() {
        try {
            BasicTest.newPermazen(ForwardDeleteConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface ForwardDeleteConflict1 {
        @PermazenField(forwardDelete = true)
        ForwardDeleteConflict getRef();
    }
    public interface ForwardDeleteConflict2 {
        @PermazenField(forwardDelete = false)
        ForwardDeleteConflict getRef();
    }
    @PermazenType
    public interface ForwardDeleteConflict extends ForwardDeleteConflict1, ForwardDeleteConflict2 {
        void setRef(ForwardDeleteConflict x);
    }

// Unique conflict

    @Test
    public void testUniqueConflict() {
        try {
            BasicTest.newPermazen(UniqueConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface UniqueConflict1 {
        @PermazenField(unique = false)
        UniqueConflict getRef();
    }
    public interface UniqueConflict2 {
        @PermazenField(unique = true)
        UniqueConflict getRef();
    }
    @PermazenType
    public interface UniqueConflict extends UniqueConflict1, UniqueConflict2 {
        void setRef(UniqueConflict x);
    }

// Unique exclude conflict

    @Test
    public void testUniqueExcludeConflict() {
        try {
            BasicTest.newPermazen(UniqueExcludeConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface UniqueExcludeConflict1 {
        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values({ "+Infinity", "-Infinity", "NaN" }))
        float getFloat();
    }
    public interface UniqueExcludeConflict2 {
        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values({ "+Infinity", "-Infinity" }))
        float getFloat();
    }
    @PermazenType
    public interface UniqueExcludeConflict extends UniqueExcludeConflict1, UniqueExcludeConflict2 {
        void setFloat(float x);
    }

// UniqueExcludeNull conflict

    @Test
    public void testUniqueExcludeNullConflict() {
        try {
            BasicTest.newPermazen(UniqueExcludeNullConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface UniqueExcludeNullConflict1 {
        @PermazenField(indexed = true, unique = true)
        Float getFloat();
    }
    public interface UniqueExcludeNullConflict2 {
        @PermazenField(indexed = true, unique = true, uniqueExcludes = @Values(nulls = true))
        Float getFloat();
    }
    @PermazenType
    public interface UniqueExcludeNullConflict extends UniqueExcludeNullConflict1, UniqueExcludeNullConflict2 {
        void setFloat(Float x);
    }

// UpgradeConversion conflict

    @Test
    public void testUpgradeConversionConflict() {
        try {
            BasicTest.newPermazen(UpgradeConversionConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface UpgradeConversionConflict1 {
        @PermazenField(upgradeConversion = UpgradeConversionPolicy.ATTEMPT)
        String getString();
    }
    public interface UpgradeConversionConflict2 {
        @PermazenField(upgradeConversion = UpgradeConversionPolicy.REQUIRE)
        String getString();
    }
    @PermazenType
    public interface UpgradeConversionConflict extends UpgradeConversionConflict1, UpgradeConversionConflict2 {
        void setString(String x);
    }

// AllowDeleted conflict

    @Test
    public void testAllowDeletedConflict() {
        try {
            BasicTest.newPermazen(AllowDeletedConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface AllowDeletedConflict1 {
        @PermazenField(allowDeleted = true)
        AllowDeletedConflict getRef();
    }
    public interface AllowDeletedConflict2 {
        @PermazenField(allowDeleted = false)
        AllowDeletedConflict getRef();
    }
    @PermazenType
    public interface AllowDeletedConflict extends AllowDeletedConflict1, AllowDeletedConflict2 {
        void setRef(AllowDeletedConflict x);
    }

// ForwardCascades conflict

    @Test
    public void testForwardCascadesConflict() {
        try {
            BasicTest.newPermazen(ForwardCascadesConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface ForwardCascadesConflict1 {
        @PermazenField(forwardCascades = "foo")
        ForwardCascadesConflict getRef();
    }
    public interface ForwardCascadesConflict2 {
        @PermazenField(forwardCascades = { })
        ForwardCascadesConflict getRef();
    }
    @PermazenType
    public interface ForwardCascadesConflict extends ForwardCascadesConflict1, ForwardCascadesConflict2 {
        void setRef(ForwardCascadesConflict x);
    }

// InverseCascades conflict

    @Test
    public void testInverseCascadesConflict() {
        try {
            BasicTest.newPermazen(InverseCascadesConflict.class);
            assert false : "expected exception";
        } catch (IllegalArgumentException e) {
            assert e.getMessage().matches("two or more methods.*conflict.*") : "wrong exception: " + e;
            this.log.debug("got expected exception: {}", e.toString());
        }
    }

    public interface InverseCascadesConflict1 {
        @PermazenField(inverseCascades = "foo")
        InverseCascadesConflict getRef();
    }
    public interface InverseCascadesConflict2 {
        @PermazenField(inverseCascades = { "foo", "bar" })
        InverseCascadesConflict getRef();
    }
    @PermazenType
    public interface InverseCascadesConflict extends InverseCascadesConflict1, InverseCascadesConflict2 {
        void setRef(InverseCascadesConflict x);
    }
}
