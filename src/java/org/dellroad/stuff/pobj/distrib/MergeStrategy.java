
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.distrib;

import java.util.List;

/**
 * Available merge strategies for a {@link GitRepository} merge operation.
 */
public enum MergeStrategy {
    RESOLVE {
        @Override
        public void addOptions(List<String> args) {
            args.add("--strategy=resolve");
        }
    },
    RECURSIVE {
        @Override
        public void addOptions(List<String> args) {
            args.add("--strategy=recursive");
        }
    },
    RECURSIVE_OURS {
        @Override
        public void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=ours");
        }
    },
    RECURSIVE_THEIRS {
        @Override
        public void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=theirs");
        }
    },
    RECURSIVE_PATIENCE {
        @Override
        public void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=patience");
        }
    },
    RECURSIVE_PATIENCE_OURS {
        @Override
        public void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=patience");
            args.add("--strategy-option=ours");
        }
    },
    RECURSIVE_PATIENCE_THEIRS {
        @Override
        public void addOptions(List<String> args) {
            args.add("--strategy=recursive");
            args.add("--strategy-option=patience");
            args.add("--strategy-option=theirs");
        }
    };

    /**
     * Add the appropriate {@code git merge} command line flags.
     */
    public abstract void addOptions(List<String> args);
}

