
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.distrib;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.dellroad.stuff.pobj.PersistentObject;
import org.dellroad.stuff.pobj.PersistentObjectEvent;
import org.dellroad.stuff.pobj.PersistentObjectException;
import org.dellroad.stuff.pobj.PersistentObjectListener;
import org.dellroad.stuff.pobj.PersistentObjectVersionException;
import org.dellroad.stuff.spring.AbstractBean;

/**
 * Synchronizes a {@link PersistentObject} database across multiple nodes using {@code git(1)}.
 *
 * <p>
 * This class provides a simple and lightweight way to build a "master-master" multi-node database
 * based on {@link PersistentObject} object graphs. The basic idea is to apply {@code git(1)}'s
 * efficient communication protocol and powerful merging algorithms to the {@link PersistentObject}
 * database's XML representation. As a result, all nodes can make simultaneous changes, updates are
 * communicated quickly, and in only a small corner case -- two conflicting changes to the same
 * data element applied at virtually same time -- must one node's change "win" and the other's "lose".
 * </p>
 *
 * <p>
 * This class works by registering as a {@linkplain PersistentObjectListener listener} on a {@link PersistentObject}
 * and also managing a private {@code git(1)} repository which is configured to track zero or more remotes.
 * Each time a change occurs to the {@link PersistentObject} database, an updated XML copy of the database
 * is committed to the {@code git(1)} repository, and then <i>synchronization</i> is performed: all remotes are fetched
 * and their corresponding {@code git(1)} branches merged into the local {@code git(1)} branch (this process
 * is performed by {@link #synchronize}). During synchronization, several merge strategies are tried until an XML file
 * is produced that {@linkplain org.dellroad.stuff.pobj.PersistentObjectDelegate#validate validates}.
 * If synchronization results in a non-trivial merge (i.e., any changes), the local {@link PersistentObject} is updated
 * with the result and therefore made aware of the other nodes' changes. In this way, all nodes have their
 * {@link PersistentObject} databases automatically synchronized and merged as they evolve.
 * </p>
 *
 * <p>
 * The user of this class must separately initialize each node's {@code git(1)} repository, configure the
 * {@linkplain #setRemotes remotes}, and determine when to {@link #synchronize} remote changes:
 * as mentioned above, synchronization is performed automatically when the local {@link PersistentObject} database changes,
 * but for remote changes either some external notification process must trigger a call to {@link #synchronize},
 * or {@link #synchronize} must be invoked at regular intervals, or (for robustness in the face of transient
 * network problems) both.
 * </p>
 *
 * <p>
 * Merges are attempted using the following strategies (in this order) until there are no merge conflicts
 * and a {@linkplain org.dellroad.stuff.pobj.PersistentObjectDelegate#validate valid} XML file is achieved:
 *  <ul>
 *  <li>Recursive merge using the patience algorithm</li>
 *  <li>Same as previous, but resolving all conflicts one way or the other</li>
 *  <li>Simply choosing one file or the other</li>
 *  </ul>
 * </p>
 *
 * <p>
 * If conflicts occur in attempt #1, but attempt #2 or #3 succeeds and the other node is the "winner", then
 * {@link #handleConflictOverride handleConflictOverride()} is invoked to notify the local node that some of
 * its recent changes have been overridden; if all three attempts fail, then
 * {@link #handleImpossibleMerge handleImpossibleMerge()} is invoked and we stay with whatever version we are on;
 * this latter case can only occur when another node is running a different version of the software
 * (with different validation criteria).
 * </p>
 *
 * <p>
 * Because all nodes follow the same algorithm for merging, they will eventually agree on the same merged result
 * (note: the {@link PersistentObject} object graph must serialize in a deterministic way for this to be true).
 * </p>
 *
 * <p>
 * Instances of this class are thread safe.
 * </p>
 *
 * @param <T> type of the root object
 */
public class Synchronizer<T> extends AbstractBean implements PersistentObjectListener<T> {

    /**
     * Default value for the XML file that is stored in the {@code git(1)} repository ({@value #DEFAULT_FILENAME}).
     */
    public static final String DEFAULT_FILENAME = "root.xml";

    /**
     * Default value for the {@code git(1)} {@linkplain #setBranch branch} name ({@value #DEFAULT_BRANCH}).
     */
    public static final String DEFAULT_BRANCH = "master";

    // Configuration info
    private PersistentObject<T> persistentObject;
    private GitRepository git;
    private String filename = DEFAULT_FILENAME;
    private String branch = DEFAULT_BRANCH;
    private List<String> remotes;

    /**
     * Configure the {@link PersistentObject} that this instance will interact with.
     *
     * <p>
     * Required property.
     * </p>
     */
    public void setPersistentObject(PersistentObject<T> persistentObject) {
        this.persistentObject = persistentObject;
    }

    /**
     * Configure the {@code git(1)} repository location. The repository, branches, and remotes must already exist.
     *
     * <p>
     * Note that multiple instances of this class can share the same {@link GitRepository} as long as they
     * are configured with different {@linkplain #setBranch branches} and/or {@linkplain #setFilename filenames}.
     * </p>
     *
     * <p>
     * Required property.
     * </p>
     *
     * @param git {@code git(1)} repository
     */
    public void setGitRepository(GitRepository git) {
        this.git = git;
    }

    /**
     * Configure the name of the XML file in the {@code git(1)} repository.
     *
     * <p>
     * Default value is {@value #DEFAULT_FILENAME}.
     * </p>
     *
     * @param filename XML file name
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * Configure name of the {@code git(1)} branches we will manage.
     *
     * <p>
     * The same branch name is used for all nodes in the cluster, local and remote, so
     * this name identifes both the local branch representing this node's root and each
     * remote's branch that we track and merge from.
     * </p>
     *
     * <p>
     * Value must match {@link GitRepository#BRANCH_NAME_PATTERN}.
     * </p>
     *
     * <p>
     * Default value is {@value #DEFAULT_BRANCH}.
     * </p>
     *
     * @param branch branch name
     */
    public void setBranch(String branch) {
        this.branch = branch;
    }

    /**
     * Configure the names of the {@code git(1)} remotes we will synchronize with.
     * Note: these are {@code git(1)} remote names, not {@code git(1)} branch names; the same
     * {@linkplain #setBranch branch} is used for all nodes in the cluster, local and remote.
     * </p>
     *
     * <p>
     * Each value must match {@link GitRepository#REMOTE_NAME_PATTERN}.
     * </p>
     *
     * <p>
     * Required property.
     * </p>
     */
    public void setRemotes(List<String> remotes) {
        this.remotes = remotes;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();

        // Sanity check
        if (this.persistentObject == null)
            throw new Exception("no PersistentObject configured");
        if (this.git == null)
            throw new Exception("no GitRepository configured");
        if (this.filename == null)
            throw new Exception("no filename configured");
        if (this.branch == null)
            throw new Exception("no branch name configured");
        if (!GitRepository.BRANCH_NAME_PATTERN.matcher(this.branch).matches())
            throw new Exception("illegal branch name `" + this.branch + "'");
        if (this.remotes == null)
            throw new Exception("no remotes configured");
        for (String remote : this.remotes) {
            if (remote == null || !GitRepository.REMOTE_NAME_PATTERN.matcher(remote).matches())
                throw new Exception("illegal remote name `" + remote + "'");
        }

        // Listen for POBJ changes
        this.persistentObject.addListener(this);

        // Do the first update
        final PersistentObject<T>.Snapshot snapshot = this.persistentObject.getSharedRootSnapshot();
        this.applyLocalUpdate(snapshot, "Initial update after startup (version " + snapshot.getVersion() + ")");
    }

    @Override
    public void destroy() throws Exception {
        this.persistentObject.removeListener(this);
        this.git = null;
        super.destroy();
    }

    @Override
    public void handleEvent(PersistentObjectEvent<T> event) {
        this.applyLocalUpdate(this.persistentObject.new Snapshot(event.getNewRoot(), event.getVersion()),
          "Update after local change (version " + event.getVersion() + ")");
    }

    /**
     * Given a (possibly) updated local root, {@linkplain #commit commit} it to our local branch,
     * {@link #synchronize} with our peers if it represented a change, and apply the newly merged
     * root (if any) back to the associated {@link PersistentObject}.
     *
     * <p>
     * This method is invoked at {@linkplain #afterPropertiesSet startup} and after {@linkplain #handleEvent notification}
     * of each local change.
     * <p>
     *
     * <p>
     * This method does nothing if the root is null.
     * </p>
     *
     * @param snapshot snapshot containing new root (possibly null) and its version
     * @param commitMessage message for the commit
     */
    protected synchronized void applyLocalUpdate(PersistentObject<T>.Snapshot snapshot, String commitMessage) {

        // Get root; ignore if null
        final T root = snapshot.getRoot();
        if (root == null)
            return;

        // Commit new root
        this.log.debug("committing new local root to branch `" + this.branch + "' (version " + snapshot.getVersion() + ")");
        String commit;
        try {
            commit = this.commit(root, commitMessage);
        } catch (Exception e) {
            this.log.error("error committing local root to branch `" + this.branch + "'", e);
            return;
        }

        // If nothing changed, nothing else to do
        if (commit == null) {
            this.log.debug("commit of version " + snapshot.getVersion() + " resulted in no change on branch `" + this.branch + "'");
            return;
        }
        this.log.info("committed root version " + snapshot.getVersion() + " as " + commit + " on branch `" + this.branch + "'");

        // Synchronize it; if no merge required or merge was trivial, we're done
        if (!this.synchronize())
            return;

        // Read in the new, merged object graph
        final ArrayList<T> rootList = new ArrayList<T>(1);
        String mergeCommit;
        try {
            mergeCommit = this.git.access(this.branch, new GitRepository.Accessor() {
                @Override
                public void accessWorkingCopy(File dir) {
                    rootList.add(Synchronizer.this.readXMLFile(dir, false));
                }
            });
        } catch (PersistentObjectException e) {
            this.log.error("error reading newly merged root; the local persistent object will not be updated", e);
            return;
        }

        // Apply changes to local persistent object; but if the local version has changed, do nothing;
        // we will do another merge when we get the next notification for the newer root.
        try {
            this.log.debug("applying merge commit " + mergeCommit + " to local persistent object");
            final long version = this.persistentObject.setRoot(rootList.get(0), snapshot.getVersion());
            this.log.info("successfully applied commit " + mergeCommit + " to local persistent object as version " + version);
        } catch (PersistentObjectVersionException e) {
            this.log.debug("merged root is out of date (version " + e.getExpectedVersion() + " < " + e.getActualVersion()
              + "); will wait for next notification");
        } catch (PersistentObjectException e) {
            this.log.error("error applying newly merged root; the local persistent object will not be updated", e);
        }
    }

    /**
     * Read the persistent object root value that is currently committed to our local branch.
     * This method can be used after a positive result from {@link #synchronize} to access the
     * updated root.
     *
     * @return committed root
     * @throws PersistentObjectException if the committed root cannot be read
     */
    public T getCommittedRoot() {
        final ArrayList<T> rootList = new ArrayList<T>(1);
        this.git.access(this.branch, new GitRepository.Accessor() {
            @Override
            public void accessWorkingCopy(File dir) {
                rootList.add(Synchronizer.this.readXMLFile(dir, false));
            }
        });
        return rootList.get(0);
    }

    /**
     * Commit the given root to our {@code git(1)} repository.
     *
     * <p>
     * If the local branch does not already exist, it will be created.
     * </p>
     *
     * @param root persistent object root
     * @param commitMessage message for the commit
     * @return new commit ID if commit occurred, or null if {@code root} was identical to the already-committed head of our branch
     * @throws IllegalArgumentException if {@code root} is null
     * @throws IllegalArgumentException if {@code commitMessage} is null
     */
    protected synchronized String commit(final T root, String commitMessage) {

        // Sanity check
        if (root == null)
            throw new IllegalArgumentException("null root");
        if (commitMessage == null)
            throw new IllegalArgumentException("null commitMessage");

        // Ensure branch exists
        this.git.ensureBranch(this.branch, "Empty commit as the basis for branch `" + this.branch + "'");

        // Get previous commit ID
        final String previousCommit = this.git.followReference("refs/heads/" + this.branch);

        // Commit it
        this.log.debug("committing root to local branch `" + this.branch + "'");
        final String newCommit = this.git.commit(this.branch, new GitRepository.Accessor() {
            @Override
            public void accessWorkingCopy(File dir) {
                PersistentObject.write(root, Synchronizer.this.persistentObject.getDelegate(),
                  Synchronizer.this.getXMLFile(dir));
            }
        }, commitMessage);

        // Report results
        return !newCommit.equals(previousCommit) ? newCommit : null;
    }

    /**
     * Synchronize what we have previously committed with our peers' latest information.
     *
     * <p>
     * This method fetches the configured remotes and then merges them into our local branch,
     * ensuring that the merge always yeilds a root that validates.
     * </p>
     *
     * <p>
     * If the local branch does not already exist, it will be created.
     * </p>
     *
     * <p>
     * This method does not affect the {@link PersistentObject} associated with this instance;
     * the caller is responsible for doing that if necessary (i.e., true is returned).
     * </p>
     *
     * @return true if a non-trivial merge ocurred, false if we were already up-to-date
     */
    public synchronized boolean synchronize() {

        // Fetch the latest from our remotes
        this.log.debug("beginning synchronization with remotes: " + this.remotes);
        this.git.fetch(this.remotes);

        // Ensure local branch exists
        this.git.ensureBranch(this.branch, "Empty commit as the basis for branch `" + this.branch + "'");

        // Merge our branch with each remote's branch
        boolean merged = false;
    remoteLoop:
        for (String remote : this.remotes) {

            // Get remote commit ID and date
            final String remoteRef = "refs/remotes/" + remote + "/" + this.branch;
            if (!this.git.getRepoFile(remoteRef).exists()) {
                this.log.debug("remote `" + remote + "' branch `" + this.branch + "' does not exist yet, skipping this remote");
                continue;
            }
            final String remoteCommit = this.git.followReference(remoteRef);
            final Date remoteAuthorDate = this.git.getAuthorDate(remoteRef);

            // Get local commit ID and date
            final String localRef = "refs/heads/" + this.branch;
            final String localCommit = this.git.followReference(localRef);
            final Date localAuthorDate = this.git.getAuthorDate(localRef);

            // Compare trees for equality
            if (this.git.equalTrees(localCommit, remoteCommit)) {
                this.log.debug("remote `" + remote + "' commit " + remoteCommit + " matches our branch `"
                  + this.branch + "' commit " + localCommit + ", skipping this remote");
                continue;
            }

            // Logit
            this.log.info("merging remote `" + remote + "' commit " + remoteCommit + " with local commit " + localCommit);

            // Determine who is the winner in case of conflicts
            boolean iWin = localCommit != null
              && (localAuthorDate.compareTo(remoteAuthorDate) > 0
                || (localAuthorDate.compareTo(remoteAuthorDate) == 0 && localCommit.compareTo(remoteCommit) > 0));

            // Build merge strategy list
            final MergeStrategy[] strategyList = new MergeStrategy[] {
              MergeStrategy.RECURSIVE_PATIENCE,
              iWin ? MergeStrategy.RECURSIVE_PATIENCE_OURS : MergeStrategy.RECURSIVE_PATIENCE_THEIRS,
              iWin ? MergeStrategy.RECURSIVE_OURS : MergeStrategy.RECURSIVE_THEIRS
            };

            // Attempt merges using successively less "mergey" strategies
            for (MergeStrategy strategy : strategyList) {

                // Attempt merge
                this.log.debug("attempting merge of remote `" + remote + "' commit " + remoteCommit
                  + " with local commit " + localCommit + " using strategy " + strategy);
                String commit;
                try {
                    commit = this.git.merge(this.branch, remoteCommit, strategy, new GitRepository.Accessor() {
                        @Override
                        public void accessWorkingCopy(File dir) {
                            Synchronizer.this.log.debug("validating merged file `" + Synchronizer.this.getXMLFile(dir) + "'");
                            Synchronizer.this.readXMLFile(dir, true);
                        }
                    }, "Merged remote `" + remote + "' commit " + remoteCommit + " using strategy " + strategy);
                } catch (GitMergeConflictException e) {
                    this.log.debug("merge using strategy " + strategy + " failed with conflict(s)");
                    continue;
                } catch (PersistentObjectException e) {
                    this.log.debug("merge using strategy " + strategy + " did not validate: " + e);
                    continue;
                }

                // We have a winner
                if (commit.equals(localCommit)) {
                    this.log.debug("merge of remote `" + remote + "' commit " + remoteCommit + " with local commit "
                      + localCommit + " using strategy " + strategy + " resulted in no change to our version");
                } else {
                    this.log.debug("successfully merged remote `" + remote + "' commit " + remoteCommit + " with local commit "
                      + localCommit + " using strategy " + strategy + " resulting in commit " + commit);
                    merged = true;
                }

                // Log a warning if some of our local changes got overridden by remote
                switch (strategy) {
                case RECURSIVE_PATIENCE_THEIRS:
                case RECURSIVE_THEIRS:
                    this.handleConflictOverride(remote, localCommit, remoteCommit, strategy);
                    break;
                default:
                    break;
                }

                // Done with this remote
                continue remoteLoop;
            }

            // Nothing worked - so we must be running different versions of the application
            this.handleImpossibleMerge(remote, localCommit, remoteCommit);
        }

        // Done
        this.log.debug("completed synchronization with remotes " + this.remotes
          + " (" + (merged ? "a" : "no") + " merge was required)");
        return merged;
    }

    /**
     * Handle a situation where some of our local changes were overridden due to merge conflicts, but
     * in the end we were able to produce a valid root.
     *
     * <p>
     * The implementation in {@link Synchronizer} just logs a warning message.
     * </p>
     *
     * @param remote name of the remote that overrode us
     * @param localCommit our commit in the merge
     * @param remoteCommit remote's commit in the merge
     * @param strategy the strategy that was used to resolve the conflict
     */
    protected void handleConflictOverride(String remote, String localCommit, String remoteCommit, MergeStrategy strategy) {
        this.log.warn("some changes in local commit " + localCommit + " have been overridden by changes from remote `"
          + remote + "' commit " + remoteCommit + " using merge strategy " + strategy);
    }

    /**
     * Handle a situation where all of the merge strategies failed to produce a valid root object.
     * This can only happen when a remote node has different validation criteria than we do, for example,
     * it is running a different version of the application.
     *
     * <p>
     * The implementation in {@link Synchronizer} just logs a warning message.
     * </p>
     *
     * @param remote name of the remote that we are conflicting with
     * @param localCommit our commit in the merge
     * @param remoteCommit remote's commit in the merge
     */
    protected void handleImpossibleMerge(String remote, String localCommit, String remoteCommit) {
        this.log.error("unable to merge local commit " + localCommit + " with remote `" + remote + "' commit "
          + remoteCommit + "; are we running different application versions?");
    }

    /**
     * Get the XML file.
     *
     * <p>
     * To avoid race conditions, the file should only be accessed from within a {@link GitRepository.Accessor} callback.
     * </p>
     *
     * @param dir {@code git(1)} working directory root
     */
    protected File getXMLFile(File dir) {
        return new File(dir, this.filename);
    }

    /**
     * Read and (optionally) validate the XML file.
     *
     * <p>
     * To avoid race conditions, this method should only be invoked from within a {@link GitRepository.Accessor} callback.
     * </p>
     *
     * @param dir {@code git(1)} working directory root
     * @param validate whether to also validate the file
     */
    protected T readXMLFile(File dir, boolean validate) {
        return PersistentObject.read(this.persistentObject.getDelegate(), this.getXMLFile(dir), validate);
    }
}

