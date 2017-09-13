
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;
import io.permazen.core.ObjId;
import io.permazen.core.util.ObjIdMap;
import io.permazen.core.util.ObjIdSet;
import io.permazen.test.TestSupport;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CopyCascadeTest extends TestSupport {

    @Test
    public void testNullCopyCascade() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Node.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            final Node n1 = jtx.create(Node.class);
            assert n1.exists();

            final JTransaction stx = jtx.getSnapshotTransaction();
            final Node n2 = (Node)n1.cascadeCopyTo(stx, null, 0, false);
            assert n2.exists();

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

    @Test
    public void testCopyCascades() throws Exception {
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Node.class, Other.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        final SnapshotJTransaction sjtx = jtx.getSnapshotTransaction();
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

            c.cascadeCopyOut("ancestors", false);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 2);
            Assert.assertEquals(sa.exists(), true);
            Assert.assertEquals(sb.exists(), false);
            Assert.assertEquals(sc.exists(), true);
            Assert.assertEquals(sd.exists(), false);
            Assert.assertSame(sa.getParent(), null);
            Assert.assertSame(sc.getParent(), sa);

            sjtx.getAll(JObject.class).stream().forEach(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

            c.cascadeCopyOut("descendants", false);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 2);
            Assert.assertEquals(sa.exists(), false);
            Assert.assertEquals(sb.exists(), false);
            Assert.assertEquals(sc.exists(), true);
            Assert.assertEquals(sd.exists(), true);
            Assert.assertSame(sc.getParent(), sa);
            Assert.assertSame(sd.getParent(), sc);

            sjtx.getAll(JObject.class).stream().forEach(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

            c.cascadeCopyOut("tree", false);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 4);
            Assert.assertEquals(sjtx.get(a).exists(), true);
            Assert.assertEquals(sjtx.get(b).exists(), true);
            Assert.assertEquals(sjtx.get(c).exists(), true);
            Assert.assertEquals(sjtx.get(d).exists(), true);

            Assert.assertSame(sa.getParent(), null);
            Assert.assertSame(sb.getParent(), sa);
            Assert.assertSame(sc.getParent(), sa);
            Assert.assertSame(sd.getParent(), sc);

            sjtx.getAll(JObject.class).stream().forEach(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

        // Check inverse cascades exclude references which do not have the cascade even if the field has the same storage ID

            final Other other = jtx.create(Other.class);
            final Other sother = sjtx.get(other);

            other.setNodeRef(a);

            a.cascadeCopyOut("tree", false);

            Assert.assertTrue(sa.exists());
            Assert.assertFalse(sother.exists());

            sjtx.getAll(JObject.class).stream().forEach(JObject::delete);
            Assert.assertEquals(sjtx.getAll(Object.class).size(), 0);

        // Check cascades copies with recursion limits

            final ObjId ia = a.getObjId();
            final ObjId ib = b.getObjId();
            final ObjId ic = c.getObjId();
            final ObjId id = d.getObjId();

            final ObjIdSet ids = new ObjIdSet();

            ids.clear();
            jtx.cascadeFindAll(ia, "tree", -1, ids);
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            jtx.cascadeFindAll(ia, "tree", 0, ids);
            Assert.assertEquals(ids, buildSet(ia));

            ids.clear();
            jtx.cascadeFindAll(ia, "tree", 1, ids);
            Assert.assertEquals(ids, buildSet(ia, ib, ic));

            ids.clear();
            jtx.cascadeFindAll(ia, "tree", 2, ids);
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            jtx.cascadeFindAll(ib, "tree", 2, ids);
            Assert.assertEquals(ids, buildSet(ia, ib, ic));

            ids.clear();
            jtx.cascadeFindAll(ib, "tree", 3, ids);
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            jtx.cascadeFindAll(ib, "tree", Integer.MAX_VALUE, ids);
            Assert.assertEquals(ids, buildSet(ia, ib, ic, id));

            ids.clear();
            jtx.cascadeFindAll(ib, "ancestors", Integer.MAX_VALUE, ids);
            Assert.assertEquals(ids, buildSet(ia, ib));

        // Done

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType
    public interface Node extends JObject {

        /**
         * Get the parent of this node, or null if node is the root.
         */
        @JField(storageId = 10, onDelete = DeleteAction.DELETE,
          cascades = { "tree", "ancestors" }, inverseCascades = { "tree", "descendants" })
        Node getParent();
        void setParent(Node x);
    }

    @PermazenType
    public interface Other extends JObject {

        @JField(storageId = 10, onDelete = DeleteAction.DELETE)
        Node getNodeRef();
        void setNodeRef(Node x);
    }
}
