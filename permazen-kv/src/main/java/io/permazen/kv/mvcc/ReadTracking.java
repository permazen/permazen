
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.mvcc;

import io.permazen.kv.KVStore;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implemented by {@link KVStore}'s that are capable of tracking which keys have been read.
 * Typically this is done to support MVCC conflict detection.
 *
 * <p>
 * Read tracking includes all keys explicitly or implicitly read by calls to
 * {@link #get get()}, {@link #getAtLeast getAtLeast()}, {@link #getAtMost getAtMost()}, and {@link #getRange getRange()}.
 *
 * <p>
 * When reads are being tracked, tracking may temporarily be paused and later resumed via {@link #getReadTrackingControl}.
 */
public interface ReadTracking extends KVStore {

    /**
     * Get an {@link AtomicBoolean} that can be used to temporarily pause/un-pause read tracking.
     *
     * <p>
     * By default the returned control is true. While set to false, read tracking is disabled; setting back
     * to true re-enables read tracking.
     *
     * <p>
     * For re-entrance safety, this should be done as follows:
     *  <blockquote><code>
     *  final boolean previous = kv.getReadTrackingControl().getAndSet(false);
     *  try {
     *      // do something without tracking reads...
     *  } finally {
     *      kv.getReadTrackingControl().set(previous);
     *  }
     *  </code></blockquote>
     *
     * @return control that enables/disables read tracking
     */
    AtomicBoolean getReadTrackingControl();
}
