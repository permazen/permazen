
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.distrib;

import java.io.File;
import java.util.List;

/**
 * Available merge strategies for a {@link GitRepository} merge operation.
 */
public enum MergeStrategy {

    /**
     * Use our version; discard other version.
     */
    OURS {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=ours");
        }
    },

    /**
     * Use other version; discard our version.
     */
    THEIRS {

        // Note, this would be easy with `--stragegy=theirs' except for this jerk: http://marc.info/?l=git&m=121637513604413&w=2
        // Links:
        //  http://stackoverflow.com/a/4969679/263801
        //  http://stackoverflow.com/a/4912267/263801
        @Override
        public void merge(File dir, String other) {

            // Sanity check
            if (other == null)
                throw new IllegalArgumentException("null other");

            // Simulate --stragegy=theirs
            new GitCommand(dir, "merge", "--no-commit", "--no-ff", "--strategy=ours", "--", other).run();
            new GitCommand(dir, "read-tree", "--reset", other).run();
        }

        @Override
        void addOptions(List<String> args) {
            throw new UnsupportedOperationException();
        }
    },

    /**
     * Use the traditional `resolve' merge algorithm.
     */
    RESOLVE {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=resolve");
        }
    },

    /**
     * Use the `recursive' merge algorithm.
     */
    RECURSIVE {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=recursive");
        }
    },

    /**
     * Use the `recursive' merge algorithm, resolving conflicts in favor of our branch.
     */
    RECURSIVE_OURS {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=ours");
        }
    },

    /**
     * Use the `recursive' merge algorithm, resolving conflicts in favor of the other branch.
     */
    RECURSIVE_THEIRS {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=theirs");
        }
    },

    /**
     * Use the `recursive' merge algorithm with the `patience' option.
     */
    RECURSIVE_PATIENCE {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=patience");
        }
    },

    /**
     * Use the `recursive' merge algorithm with the `patience' option, resolving conflicts in favor of our branch.
     */
    RECURSIVE_PATIENCE_OURS {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=patience");
            args.add("--strategy-option=ours");
        }
    },

    /**
     * Use the `recursive' merge algorithm with the `patience' option, resolving conflicts in favor of the other branch.
     */
    RECURSIVE_PATIENCE_THEIRS {
        @Override
        void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=patience");
            args.add("--strategy-option=theirs");
        }
    };

    /**
     * Peform a merge using this strategy, without committing.
     * This assumes the checkout directory is prepared appropriately for the merge with the target branch checked out.
     *
     * @param dir checkout directory
     * @param other other branch to merge with
     * @throws IllegalArgumentException if {@code dir} or {@code other} is null
     * @throws GitMergeConflictException if a merge conflict occurs
     * @throws GitException if an error other than a merge conflict occurs
     */
    public void merge(File dir, String other) {

        // Run merge command
        final GitCommand merge = this.buildMergeCommand(dir, other);
        final int exitValue = merge.run(true);

        // Any conflict or other error?
        if (exitValue == 0)
            return;

        // Check for errors other than conflict
        final File mergeHeadFile = new File(new File(dir, ".git"), "MERGE_HEAD");
        if (!mergeHeadFile.exists())
            throw new GitException("git merge failed: " + merge.getStandardError().trim());

        // Abort the merge
        new GitCommand(dir, "merge", "--abort").run();

        // Bail out
        throw new GitMergeConflictException("merge failed with conflict(s)");
    }

    /**
     * Build the merge command. Delegates to {@link #addOptions} for strategy-specific merge command options.
     *
     * @param dir checkout directory
     * @param other other branch to merge with
     * @throws IllegalArgumentException if {@code dir} or {@code other} is null
     */
    private GitCommand buildMergeCommand(File dir, String other) {

        // Sanity check
        if (other == null)
            throw new IllegalArgumentException("null other");

        // Build command
        final GitCommand merge = new GitCommand(dir, "merge", "--no-commit", "--no-ff");
        this.addOptions(merge.getArgs());
        merge.getArgs().add("--");
        merge.getArgs().add(other);
        return merge;
    }

    /**
     * Add the appropriate {@code git merge} command line flags.
     */
    abstract void addOptions(List<String> args);
}

