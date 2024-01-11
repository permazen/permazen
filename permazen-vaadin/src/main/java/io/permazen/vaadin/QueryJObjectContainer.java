
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.collect.Iterators;

import io.permazen.CopyState;
import io.permazen.PermazenObject;
import io.permazen.PermazenTransaction;
import io.permazen.Permazen;
import io.permazen.SnapshotJTransaction;
import io.permazen.ValidationMode;
import io.permazen.core.UnknownFieldException;

import java.util.Iterator;

/**
 * Vaadin {@link com.vaadin.data.Container} backed by {@link Permazen} Java model objects acquired by performing
 * a transactional query and copying out the results into a {@link SnapshotJTransaction}.
 *
 * <p>
 * The subclass method {@linkplain #queryForObjects queryForObjects()} performs the query returning the backing objects
 * within an already-opened transaction.  The {@link com.vaadin.data.Item}s in the container are backed by in-memory copies
 * of the returned {@link PermazenObject}s; these live inside a {@link SnapshotJTransaction} and therefore may persist indefinitely
 * after the query transaction closes.
 *
 * <p>
 * Instances may be (re)loaded at any time by invoking {@link #reload}. During reload, the container opens a new
 * {@link PermazenTransaction}, queries for objects using {@link #queryForObjects queryForObjects()}, and copies each returned
 * object into the associated {@link SnapshotJTransaction} via {@link #copyWithRelated copyWithRelated()},
 * which copies the object and any related objects necessary for resolving container properties. The set of related
 * objects associated with an object is determined by {@link #getRelatedObjects getRelatedObjects()}; by default,
 * this is all directly referenced objects. Subclasses may override to customize.
 *
 * <p>
 * It is up to {@link #queryForObjects queryForObjects()} to determine what and how many objects are returned, their
 * sort order, etc. Any objects returned by {@link #queryForObjects queryForObjects()} that are not instances of the
 * container's {@linkplain #setType configured type} are ignored.
 *
 * <p>
 * <b>{@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty} Limitations</b>
 *
 * <p>
 * The use of {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty} methods has certain implications.
 * First, if the method reads any of the object's field(s) via their Java getter methods (as would normally be expected),
 * this will trigger a schema upgrade of the object if needed; however, this schema upgrade will occur in the
 * container's in-memory {@link SnapshotJTransaction} rather than in a real database transaction, so the
 * {@link #VERSION_PROPERTY} will return a different schema version from what's in the database. The automatic schema
 * upgrade can be avoided if desired by reading the field using the appropriate {@link PermazenTransaction} field access method
 * (e.g., {@link PermazenTransaction#readSimpleField PermazenTransaction.readSimpleField()}) and being prepared to handle a
 * {@link UnknownFieldException} if/when the object has an older schema version that does not contain
 * the requested field.
 *
 * <p>
 * Secondly, because the values of reference fields (including complex sub-fields) are displayed using reference labels,
 * and these are typically derived from the referenced object's fields, those indirectly referenced objects need to be
 * copied into the container's {@link SnapshotJTransaction} as well. The easiest way to ensure these indirectly
 * referenced objects are copied is by overriding {@link #getRelatedObjects getRelatedObjects()} as described above.
 */
@SuppressWarnings("serial")
public abstract class QueryJObjectContainer extends ReloadableJObjectContainer {

    /**
     * Constructor.
     *
     * @param jdb {@link Permazen} database
     * @param type type restriction, or null for no restriction
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    protected QueryJObjectContainer(Permazen jdb, Class<?> type) {
        super(jdb, type);
    }

    /**
     * (Re)load this container.
     *
     * <p>
     * This creates a new {@link PermazenTransaction}, invokes {@link #queryForObjects} to query for backing objects,
     * copies them into an in-memory {@link SnapshotJTransaction} via {@link #copyWithRelated copyWithRelated()},
     * and builds the container from the result.
     */
    @Override
    public void reload() {
        this.doInTransaction(this::reloadInTransaction);
    }

    // This method runs within a transaction
    private void reloadInTransaction() {

        // Get objects from subclass
        Iterator<? extends PermazenObject> jobjs = this.queryForObjects();

        // Copy objects (and their related object friends) into a snapshot transaction
        final SnapshotJTransaction snapshotTx = PermazenTransaction.getCurrent().createSnapshotTransaction(ValidationMode.DISABLED);
        final CopyState copyState = new CopyState();
        jobjs = Iterators.transform(jobjs, jobj -> this.copyWithRelated(jobj, snapshotTx, copyState));

        // Now actually load the objects
        this.load(jobjs);
    }

    /**
     * Copy the given database object, and any related objects needed by any
     * {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty}-annotated methods,
     * into the specified transaction.
     *
     * <p>
     * The implementation in {@link JObjectContainer} copies {@code jobj}, and all of {@code jobj}'s related objects returned
     * by {@link #getRelatedObjects getRelatedObjects()}, via {@link PermazenTransaction#copyTo(PermazenTransaction, CopyState, Iterable)}.
     *
     * @param target the object to copy, or null (ignored)
     * @param dest destination transaction
     * @param copyState tracks what's already been copied
     * @return the copy of {@code target} in {@code dest}, or null if {@code target} is null
     * @see #getRelatedObjects getRelatedObjects()
     */
    public PermazenObject copyWithRelated(PermazenObject target, PermazenTransaction dest, CopyState copyState) {

        // Ignore null
        if (target == null)
            return null;

        // Copy out target object
        final PermazenTransaction jtx = target.getTransaction();
        final PermazenObject copy = target.copyTo(dest, copyState);

        // Copy out target's related objects
        final Iterable<? extends PermazenObject> relatedObjects = this.getRelatedObjects(target);
        if (relatedObjects != null)
            jtx.copyTo(dest, copyState, relatedObjects);

        // Done
        return copy;
    }

    /**
     * Find objects related to the specified object that are needed by any
     * {@link org.dellroad.stuff.vaadin7.ProvidesProperty &#64;ProvidesProperty}-annotated
     * methods.
     *
     * <p>
     * This defines all of the other objects on which any container property of {@code jobj} may depend.
     * These related objects will copied along with {@code obj} into the container's
     * {@link SnapshotJTransaction} when the container is (re)loaded.
     *
     * <p>
     * The implementation in {@link JObjectContainer} returns all objects that are directly referenced by {@code jobj},
     * delegating to {@link Permazen#getReferencedObjects Permazen.getReferencedObjects()}.
     * Subclasses may override this method to refine the selection.
     *
     * @param jobj the object being copied
     * @return {@link Iterable} of additional objects to be copied, or null for none; any null values are ignored
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    protected Iterable<? extends PermazenObject> getRelatedObjects(PermazenObject jobj) {
        return this.jdb.getReferencedObjects(jobj);
    }

    /**
     * Query for the database objects that will be used to fill this container. Objects should be returned in the
     * desired order; duplicates and null values will be ignored.
     *
     * <p>
     * A {@link PermazenTransaction} will be open in the current thread when this method is invoked.
     *
     * @return database objects
     */
    protected abstract Iterator<? extends PermazenObject> queryForObjects();
}
