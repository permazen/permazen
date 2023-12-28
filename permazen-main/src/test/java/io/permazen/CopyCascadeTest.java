
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.core.ObjId;
import io.permazen.core.util.ObjIdSet;

import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CopyCascadeTest extends MainTestSupport {

    @Test
    public void testNullCopyCascade() throws Exception {
        final Permazen jdb = BasicTest.newPermazen(Node.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Node n1 = jtx.create(Node.class);
            assert n1.exists();

            final JTransaction stx = jtx.getDetachedTransaction();
            final Node n2 = (Node)n1.copyTo(stx, 0, new CopyState());
            assert n2.exists();

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCopyCascades() throws Exception {
        final Permazen jdb = BasicTest.newPermazen(Node.class, Other.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.AUTOMATIC);
        final DetachedJTransaction sjtx = jtx.getDetachedTransaction();
        JTransaction.setCurrent(jtx);
        try {

        //
        // Tree used for test:
        //
        //        A
        //       / \
        //      B   C
        //           \
        //            D
        //

            final Node a = jtx.create(Node.class);
            final Node b = jtx.create(Node.class);
            final Node c = jtx.create(Node.class);
            final Node d = jtx.create(Node.class);

            b.setParent(a);
            c.setParent(a);
            d.setParent(c);

            Assert.assertEquals(jtx.getAll(Object.class).size(), 4);

        // Check copy cascades are followed correctly

            final Node sa = sjtx.get(a);
            final Node sb = sjtx.get(b);
            final Node sc = sjtx.get(c);
            final Node sd = sjtx.get(d);

            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

            c.copyOut("ancestors");
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 2);
            Assert.assertEquals(sa.exists(), true);
            Assert.assertEquals(sb.exists(), false);
            Assert.assertEquals(sc.exists(), true);
            Assert.assertEquals(sd.exists(), false);
            Assert.assertSame(sa.getParent(), null);
            Assert.assertSame(sc.getParent(), sa);

            sjtx.getAll(JObject.class).stream().iterator().forEachRemaining(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

            c.copyOut("descendants");
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 2);
            Assert.assertEquals(sa.exists(), false);
            Assert.assertEquals(sb.exists(), false);
            Assert.assertEquals(sc.exists(), true);
            Assert.assertEquals(sd.exists(), true);
            Assert.assertSame(sc.getParent(), sa);
            Assert.assertSame(sd.getParent(), sc);

            sjtx.getAll(JObject.class).stream().iterator().forEachRemaining(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

            c.copyOut("tree");
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 4);
            Assert.assertEquals(sjtx.get(a).exists(), true);
            Assert.assertEquals(sjtx.get(b).exists(), true);
            Assert.assertEquals(sjtx.get(c).exists(), true);
            Assert.assertEquals(sjtx.get(d).exists(), true);

            Assert.assertSame(sa.getParent(), null);
            Assert.assertSame(sb.getParent(), sa);
            Assert.assertSame(sc.getParent(), sa);
            Assert.assertSame(sd.getParent(), sc);

            sjtx.getAll(JObject.class).stream().iterator().forEachRemaining(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

        // Check inverse cascades exclude references which do not have the cascade even if the field has the same storage ID

            final Other other = jtx.create(Other.class);
            final Other sother = sjtx.get(other);

            other.setNodeRef(a);

            a.copyOut("tree");

            Assert.assertTrue(sa.exists());
            Assert.assertFalse(sother.exists());

            sjtx.getAll(JObject.class).stream().iterator().forEachRemaining(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

        // Check cascades copies with recursion limits

            final ObjId ia = a.getObjId();
            final ObjId ib = b.getObjId();
            final ObjId ic = c.getObjId();
            final ObjId id = d.getObjId();

            final ObjIdSet ids = new ObjIdSet();

            ids.clear();
            exhaust(jtx.cascade(ia, -1, ids, "tree"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            exhaust(jtx.cascade(ia, 0, ids, "tree"));
            Assert.assertEquals(ids, buildSet(ia));

            ids.clear();
            exhaust(jtx.cascade(ia, 1, ids, "tree"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic));

            ids.clear();
            exhaust(jtx.cascade(ia, 2, ids, "tree"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            exhaust(jtx.cascade(ib, 2, ids, "tree"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic));

            ids.clear();
            exhaust(jtx.cascade(ib, 3, ids, "tree"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            exhaust(jtx.cascade(ib, Integer.MAX_VALUE, ids, "tree"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            exhaust(jtx.cascade(ib, Integer.MAX_VALUE, ids, "ancestors"));
            Assert.assertEquals(ids, buildSet(ia, ib));

        // Check cascade through multiple cascade names

            ids.clear();
            exhaust(jtx.cascade(ic, 0, ids, "ancestors", "descendants"));
            Assert.assertEquals(ids, buildSet(ic));

            ids.clear();
            exhaust(jtx.cascade(ic, 1, ids, "ancestors", "descendants"));
            Assert.assertEquals(ids, buildSet(ia, ic, id));

            ids.clear();
            exhaust(jtx.cascade(ic, 2, ids, "ancestors", "descendants"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            exhaust(jtx.cascade(ic, 3, ids, "ancestors", "descendants"));
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

        // Done

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    private static void exhaust(Iterator<ObjId> i) {
        while (i.hasNext())
            i.next();
    }

// Model Classes

    @PermazenType
    public interface Node extends JObject {

        /**
         * Get the parent of this node, or null if node is the root.
         */
        @JField(inverseDelete = DeleteAction.DELETE,
          forwardCascades = { "tree", "ancestors" },
          inverseCascades = { "tree", "descendants" })
        Node getParent();
        void setParent(Node x);
    }

    @PermazenType
    public interface Other extends JObject {

        @JField(inverseDelete = DeleteAction.DELETE)
        Node getNodeRef();
        void setNodeRef(Node x);
    }
}
