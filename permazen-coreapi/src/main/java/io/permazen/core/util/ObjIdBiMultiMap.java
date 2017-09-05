
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.util;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import io.permazen.core.ObjId;

/**
 * A bi-directional, many-to-many mapping between {@link ObjId}s.
 *
 * <p>
 * Instances can be thought of as containing a set of <i>source, target</i> ordered pairs.
 * The many-to-many mapping can be efficiently queried and modified from either direction.
 *
 * <p>
 * Instances of this class are thread-safe.
 */
@ThreadSafe
public class ObjIdBiMultiMap implements Cloneable, Serializable {

    private static final long serialVersionUID = 2063318188143069113L;

    @GuardedBy("this")
    private /*final*/ transient Object lock;                // we always synchronize on this object
    @GuardedBy("this")
    private /*final*/ transient ObjIdBiMultiMap inverse;    // if not null, this is my inverse
    @GuardedBy("this")
    private /*final*/ ObjIdMap<ObjIdSet> forward;
    @GuardedBy("this")
    private /*final*/ ObjIdMap<ObjIdSet> reverse;

// Constructors

    /**
     * Default constructor.
     */
    public ObjIdBiMultiMap() {
        this(0, 0);
    }

    /**
     * Constructs an instance with the given initial capacities.
     *
     * @param sourceCapacity initial capacity for the number of sources
     * @param targetCapacity initial capacity for the number of targets
     * @throws IllegalArgumentException if either value is negative
     */
    public ObjIdBiMultiMap(int sourceCapacity, int targetCapacity) {
        this(null, new ObjIdMap<>(sourceCapacity), new ObjIdMap<>(targetCapacity));
    }

    // Internal constructor
    private ObjIdBiMultiMap(ObjIdBiMultiMap inverse, ObjIdMap<ObjIdSet> forward, ObjIdMap<ObjIdSet> reverse) {
        if (inverse != null) {
            this.lock = inverse.lock;
            this.inverse = inverse;
        } else {
            this.lock = new Object();
            this.inverse = null;
        }
        this.forward = forward;
        this.reverse = reverse;
    }

// Public methods

    /**
     * Get the the number of sources that have one or more associated targets.
     *
     * @return the number of sources contained in this instance
     */
    public int getNumSources() {
        synchronized (this.lock) {
            return this.forward.size();
        }
    }

    /**
     * Get the the number of targets that have one or more associated sources.
     *
     * @return the number of targets contained in this instance
     */
    public int getNumTargets() {
        synchronized (this.lock) {
            return this.reverse.size();
        }
    }

    /**
     * Get all sources associated with this instance that have one or more associated targets.
     *
     * <p>
     * The returned {@link ObjIdSet} is mutable, but changes to it do not affect this instance.
     *
     * @return all source ID's associated with this instance, possibly empty
     */
    public ObjIdSet getSources() {
        synchronized (this.lock) {
            return this.forward.keySet().clone();
        }
    }

    /**
     * Get all targets associated with this instance that have one or more associated sources.
     *
     * <p>
     * The returned {@link ObjIdSet} is mutable, but changes to it do not affect this instance.
     *
     * @return all targets ID's associated with this instance, possibly empty
     */
    public ObjIdSet getTargets() {
        synchronized (this.lock) {
            return this.reverse.keySet().clone();
        }
    }

    /**
     * Get the sources associated with the given target, if any.
     *
     * <p>
     * The returned {@link ObjIdSet} is mutable, but changes to it do not affect this instance.
     *
     * @param target target ID
     * @return one or more source ID's associated with {@code target}, or null if there are none
     * @throws IllegalArgumentException if {@code target} is null
     */
    public ObjIdSet getSources(ObjId target) {
        Preconditions.checkArgument(target != null, "null target");
        synchronized (this.lock) {
            final ObjIdSet sources = this.reverse.get(target);
            return sources != null ? sources.clone() : null;
        }
    }

    /**
     * Get the targets associated with the given source, if any.
     *
     * <p>
     * The returned {@link ObjIdSet} is mutable, but changes to it do not affect this instance.
     *
     * @param source source ID
     * @return one or more target ID's associated with {@code source}, or null if there are none
     * @throws IllegalArgumentException if {@code source} is null
     */
    public ObjIdSet getTargets(ObjId source) {
        Preconditions.checkArgument(source != null, "null source");
        synchronized (this.lock) {
            final ObjIdSet targets = this.forward.get(source);
            return targets != null ? targets.clone() : null;
        }
    }

    /**
     * Determine if this instance has any targets associated with the specified source.
     *
     * @param source source ID
     * @return true if any target ID's are associated with {@code source}, otherwise false
     * @throws IllegalArgumentException if {@code source} is null
     */
    public boolean containsSource(ObjId source) {
        Preconditions.checkArgument(source != null, "null source");
        synchronized (this.lock) {
            return this.forward.containsKey(source);
        }
    }

    /**
     * Determine if this instance has any sources associated with the specified target.
     *
     * @param target target ID
     * @return true if any source ID's are associated with {@code target}, otherwise false
     * @throws IllegalArgumentException if {@code target} is null
     */
    public boolean containsTarget(ObjId target) {
        Preconditions.checkArgument(target != null, "null target");
        synchronized (this.lock) {
            return this.reverse.containsKey(target);
        }
    }

    /**
     * Add an association.
     *
     * @param source source ID
     * @param target target ID
     * @return true if association was added, false if the association already existed
     * @throws IllegalArgumentException if {@code source} or {@code target} is null
     */
    public boolean add(ObjId source, ObjId target) {
        Preconditions.checkArgument(source != null, "null source");
        Preconditions.checkArgument(target != null, "null target");
        synchronized (this.lock) {
            if (!ObjIdBiMultiMap.add(this.forward, source, target))
                return false;
            ObjIdBiMultiMap.add(this.reverse, target, source);
            return true;
        }
    }

    /**
     * Add multiple associations with a given source.
     *
     * @param source source ID
     * @param targets target ID's
     * @return true if any association was added, false if all associations already existed
     * @throws IllegalArgumentException if {@code source}, {@code targets}, or any target in the iteration is null
     */
    public boolean addAll(ObjId source, Iterable<? extends ObjId> targets) {

        // Sanity check
        Preconditions.checkArgument(source != null, "null source");
        Preconditions.checkArgument(targets != null, "null targets");

        // Gather targets
        final ObjIdSet addedTargets = ObjIdBiMultiMap.gather(targets);
        if (addedTargets.isEmpty())
            return false;

        // Update maps
        boolean result = false;
        synchronized (this.lock) {
            final ObjIdSet targetSet = this.forward.get(source);
            if (targetSet != null) {
                assert !targetSet.isEmpty();
                if (targetSet.size() >= addedTargets.size())
                    result = targetSet.addAll(addedTargets);
                else {
                    addedTargets.addAll(targetSet);
                    this.forward.put(source, addedTargets);
                    result = true;
                }
            } else {
                this.forward.put(source, addedTargets);
                result = true;
            }
            for (ObjId target : addedTargets)
                ObjIdBiMultiMap.add(this.reverse, target, source);
        }

        // Done
        return result;
    }

    /**
     * Remove an association.
     *
     * @param source source ID
     * @param target target ID
     * @return true if association was removed, false if the association did not exist
     * @throws IllegalArgumentException if {@code source} or {@code target} is null
     */
    public boolean remove(ObjId source, ObjId target) {
        Preconditions.checkArgument(source != null, "null source");
        Preconditions.checkArgument(target != null, "null target");
        synchronized (this.lock) {
            if (!ObjIdBiMultiMap.remove(this.forward, source, target))
                return false;
            ObjIdBiMultiMap.remove(this.reverse, target, source);
            return true;
        }
    }

    /**
     * Remove multiple associations with a given source.
     *
     * @param source source ID
     * @param targets target ID's
     * @return true if any association was removed, false if none of the specified associations existed
     * @throws IllegalArgumentException if {@code source}, {@code targets}, or any target in the iteration is null
     */
    public boolean removeAll(ObjId source, Iterable<? extends ObjId> targets) {

        // Sanity check
        Preconditions.checkArgument(source != null, "null source");
        Preconditions.checkArgument(targets != null, "null targets");

        // Gather targets
        final ObjIdSet removedTargets = ObjIdBiMultiMap.gather(targets);
        if (removedTargets.isEmpty())
            return false;

        // Update maps
        boolean result = false;
        synchronized (this.lock) {
            final ObjIdSet targetSet = this.forward.get(source);
            if (targetSet == null)
                return false;
            assert !targetSet.isEmpty();
            for (ObjId target : removedTargets) {
                if (targetSet.remove(target)) {
                    final boolean removed = ObjIdBiMultiMap.remove(this.reverse, target, source);
                    assert removed;
                    result = true;
                }
            }
            if (targetSet.isEmpty())
                this.forward.remove(source);
        }

        // Done
        return result;
    }

    /**
     * Remove all associations involving the specified source.
     *
     * @param source source ID
     * @return true if any {@code source} association(s) were removed, false if {@code source} had no target associations
     * @throws IllegalArgumentException if {@code source} is null
     */
    public boolean removeSource(ObjId source) {
        Preconditions.checkArgument(source != null, "null source");
        synchronized (this.lock) {
            final ObjIdSet targets = this.forward.remove(source);
            if (targets == null)
                return false;
            assert !targets.isEmpty();
            for (ObjId target : targets) {
                final ObjIdSet sources = this.reverse.get(target);
                sources.remove(source);
                if (sources.isEmpty())
                    this.reverse.remove(target);
            }
        }
        return true;
    }

    /**
     * Remove all associations involving the specified target.
     *
     * @param target target ID
     * @return true if any {@code target} association(s) were removed, false if {@code target} had no source associations
     * @throws IllegalArgumentException if {@code target} is null
     */
    public boolean removeTarget(ObjId target) {
        return this.inverse().removeSource(target);
    }

    /**
     * Clear this instance.
     */
    public void clear() {
        synchronized (this.lock) {
            this.forward.clear();
            this.reverse.clear();
        }
    }

    /**
     * Get an inverse view backed by this instance.
     *
     * <p>
     * The returned {@link ObjIdBiMultiMap} is a <i>view</i> in which sources become targets and vice-versa;
     * any changes are reflected back in this instance.
     *
     * <p>
     * This method is efficient, requiring only constant time.
     *
     * @return inverse view of this instance
     */
    public ObjIdBiMultiMap inverse() {
        return this.inverse != null ? this.inverse : new ObjIdBiMultiMap(this, this.reverse, this.forward);
    }

// Object

    /**
     * Calculate a hash code value for this instance.
     */
    @Override
    public int hashCode() {
        synchronized (this.lock) {
            return this.forward.hashCode();
        }
    }

    /**
     * Compare for equality.
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final ObjIdBiMultiMap that = (ObjIdBiMultiMap)obj;
        final ObjIdMap<ObjIdSet> thisForward;
        final ObjIdMap<ObjIdSet> thatForward;
        synchronized (this.lock) {
            thisForward = ObjIdBiMultiMap.deepClone(this.forward);
        }
        synchronized (that.lock) {
            thatForward = ObjIdBiMultiMap.deepClone(that.forward);
        }
        return thisForward.equals(thatForward);
    }

    /**
     * Create a {@link String} representation.
     */
    @Override
    public String toString() {
        synchronized (this.lock) {
            return this.forward.toString();
        }
    }

// Cloneable

    /**
     * Clone this instance.
     */
    @Override
    @SuppressWarnings("unchecked")
    public ObjIdBiMultiMap clone() {
        final ObjIdBiMultiMap clone;
        try {
            clone = (ObjIdBiMultiMap)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
        clone.lock = new Object();
        synchronized (clone.lock) {
            synchronized (this.lock) {
                clone.forward = ObjIdBiMultiMap.deepClone(this.forward);
                clone.reverse = ObjIdBiMultiMap.deepClone(this.reverse);
            }
            clone.inverse = null;
        }
        return clone;
    }

// Internal methods

    private static ObjIdSet gather(Iterable<? extends ObjId> ids) {
        final ObjIdSet set = new ObjIdSet();
        for (ObjId id : ids) {
            Preconditions.checkArgument(id != null, "encountered null ObjId in iteration");
            set.add(id);
        }
        return set;
    }

    private static boolean add(ObjIdMap<ObjIdSet> map, ObjId source, ObjId target) {
        ObjIdSet set = map.get(source);
        if (set == null) {
            set = new ObjIdSet();
            map.put(source, set);
        }
        return set.add(target);
    }

    private static boolean remove(ObjIdMap<ObjIdSet> map, ObjId source, ObjId target) {
        final ObjIdSet set = map.get(source);
        if (set == null || !set.remove(target))
            return false;
        if (set.isEmpty())
            map.remove(source);
        return true;
    }

    private static ObjIdMap<ObjIdSet> deepClone(ObjIdMap<ObjIdSet> map) {
        final ObjIdMap<ObjIdSet> clone = map.clone();
        final int numSlots = map.getKeys().length;
        for (int i = 0; i < numSlots; i++) {
            final ObjIdSet set = map.getValue(i);
            if (set != null)
                clone.setValue(i, set.clone());
        }
        return clone;
    }

// Serialization

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
        input.defaultReadObject();
        this.lock = new Object();
    }
}
