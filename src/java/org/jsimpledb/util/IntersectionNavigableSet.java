
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.NoSuchElementException;

/**
 * Provides a read-only view of the intersection of two or more {@link NavigableSet}s.
 * A complete iteration takes <i>O(N * M)</i> queries, where <i>N</i> is the number of
 * elements in the smallest set and <i>M</i> is the number of sets.
 */
class IntersectionNavigableSet<E> extends AbstractMultiNavigableSet<E> {

    /**
     * Constructor.
     *
     * @param sets the sets to intersect
     */
    public IntersectionNavigableSet(Iterable<? extends NavigableSet<E>> sets) {
        super(sets);
    }

    /**
     * Internal constructor.
     *
     * @param sets the sets to intersect
     * @param comparator common comparator
     * @param bounds range restriction
     * @throws IllegalArgumentException if {@code bounds} is null
     */
    protected IntersectionNavigableSet(Iterable<? extends NavigableSet<E>> sets,
      Comparator<? super E> comparator, Bounds<E> bounds) {
        super(sets, comparator, bounds);
    }

    @Override
    protected NavigableSet<E> createSubSet(boolean reverse, Bounds<E> newBounds, List<NavigableSet<E>> newList) {
        final Comparator<? super E> newComparator = this.getComparator(reverse);
        return new IntersectionNavigableSet<E>(newList, newComparator, bounds);
    }

    @Override
    public boolean contains(Object obj) {
        for (NavigableSet<E> set : this.list) {
            if (!set.contains(obj))
                return false;
        }
        return true;
    }

    @Override
    public java.util.Iterator<E> iterator() {
        return new Iterator();
    }

// Iterator

    private class Iterator implements java.util.Iterator<E> {

        private final Comparator<? super E> comparator = IntersectionNavigableSet.this.getComparator(false);

        private boolean firstTime = true;
        private boolean finished = IntersectionNavigableSet.this.list.isEmpty();
        private boolean haveNext;
        private E next;

        @Override
        public boolean hasNext() {
            return this.haveNext || this.advance();
        }

        @Override
        public E next() {
            if (!this.haveNext && !this.advance())
                throw new NoSuchElementException();
            this.haveNext = false;
            return this.next;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private boolean advance() {

            // Finished?
            if (this.finished)
                return false;
            assert !this.haveNext;

            // Get initial candidate for the next iteration element
            E candidate;
            final NavigableSet<E> firstSet = IntersectionNavigableSet.this.list.get(0);
            if (this.firstTime) {
                this.firstTime = false;
                try {
                    candidate = firstSet.first();
                } catch (NoSuchElementException e) {
                    this.finished = true;
                    return false;
                }
            } else if ((candidate = firstSet.higher(this.next)) == null && !this.hasNullInTailSet(firstSet, this.next, false)) {
                this.finished = true;
                return false;
            }

            // Cycle through the sets until we have found the candidate in every set, moving candidate forward as we go
            final int maxMatches = IntersectionNavigableSet.this.list.size();
            int numMatches = 1;
            for (int i = 1; numMatches < maxMatches; i = (i + 1) % maxMatches) {
                final NavigableSet<E> set = IntersectionNavigableSet.this.list.get(i);

                // Look for candidate in the next set, or else something higher
                final E ceiling = set.ceiling(candidate);

                // Distinguish between a normal but null element, and a null value meaning "no more elements"
                if (ceiling == null && !this.hasNullInTailSet(set, candidate, true)) {
                    this.finished = true;
                    return false;
                }

                // Did we get the same candidate element back, or some higher element?
                final int diff = this.comparator.compare(ceiling, candidate);
                if (diff == 0) {
                    numMatches++;
                    continue;
                }

                // If we got back an element greater than the current candidate, it becomes the new candidate
                if (diff > 0) {
                    numMatches = 1;
                    candidate = ceiling;
                    continue;
                }

                // Oops, sets are not ordered properly
                throw new IllegalStateException("internal error: NavigableSet.ceiling() returned a mis-ordered element "
                  + ceiling + " < " + candidate);
            }

            // We found candidate in all of the sets
            this.next = candidate;
            this.haveNext = true;
            return true;
        }

        private boolean hasNullInTailSet(NavigableSet<E> set, E elem, boolean inclusive) {
            NavigableSet<E> tailSet;
            try {
                tailSet = set.tailSet(elem, inclusive);
            } catch (IllegalArgumentException e) {              // "candidate" is out of set's range, so we're done
                return false;
            }
            return !tailSet.isEmpty();
        }
    }
}

