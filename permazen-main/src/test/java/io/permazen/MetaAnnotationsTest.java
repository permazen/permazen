
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetaAnnotationsTest extends MainTestSupport {

    @Test
    public void testGenerics1() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(Mommy.class, Baby.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.AUTOMATIC);
        PermazenTransaction.setCurrent(ptx);
        try {

            final Mommy mommy = ptx.create(Mommy.class);
            final Baby baby1 = ptx.create(Baby.class);
            final Baby baby2 = ptx.create(Baby.class);

            baby1.setMommy(mommy);
            baby2.setMommy(mommy);

            try {
                ptx.validate();
                assert false;
            } catch (ValidationException e) {
                this.log.debug("got expected {}", e.toString());
            }

            baby2.setMommy(null);

            final Baby baby1copy = (Baby)baby1.copyOut("load");
            Assert.assertNotNull(baby1copy.getMommy());

            ptx.commit();

        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Meta-Annotations

    @PermazenType
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface ModelClass {
    }

    @PermazenField(inverseDelete = DeleteAction.DELETE, forwardCascades = "load")
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Parent {

        @AliasFor(annotation = PermazenField.class, attribute = "unique")
        boolean onlyChild() default false;
    }

// Model Classes

    @ModelClass
    public abstract static class Mommy implements PermazenObject {
    }

    @ModelClass
    public abstract static class Baby implements PermazenObject {

        @Parent(onlyChild = true)
        public abstract Mommy getMommy();
        public abstract void setMommy(Mommy mommy);
    }
}
