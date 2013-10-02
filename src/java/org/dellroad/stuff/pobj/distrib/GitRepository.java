
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.distrib;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dellroad.stuff.java.ProcessRunner;
import org.dellroad.stuff.spring.AbstractBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a {@code git(1)} repository and provides a few simple methods that wrap the command-line client
 * and provide the functionality needed by {@link Synchronizer}.
 * Only normal (i.e., non-bare) repositories are supported.
 *
 * <p>
 * This class assumes the repository already has all branches and remotes already configured, and that no
 * other external process is accessing the {@code git(1)} repository without our knowledge.
 * </p>
 *
 * <p>
 * Instances are thread safe and guarantees exclusive access to the working directory
 * to each {@link Accessor} while it is in use.
 * </p>
 *
 * @see <a href="http://git-scm.com/">Git web site</a>
 */
public class GitRepository extends AbstractBean {

    /**
     * Accepted git branch name pattern.
     */
    public static final Pattern BRANCH_NAME_PATTERN = Pattern.compile("[\\w][-:\\w]*");

    /**
     * Accepted git remote name pattern.
     */
    public static final Pattern REMOTE_NAME_PATTERN = Pattern.compile("[\\w][-:\\w]*");

    /**
     * SHA1 pattern.
     */
    public static final Pattern SHA1_PATTERN = Pattern.compile("[0-9a-f]{40}");

    private final File dir;

// Constructors

    /**
     * Constructor.
     *
     * @param dir repository filesystem location (i.e., the directory containing {@code .git} as a subdirectory)
     * @throws IllegalArgumentException if {@code dir} is null
     */
    public GitRepository(File dir) {
        if (dir == null)
            throw new IllegalArgumentException("null dir");
        this.dir = dir;
    }

// Lifecycle

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        final File gitDir = new File(this.dir, ".git");
        if (!gitDir.exists() || !gitDir.isDirectory())
            throw new Exception("`" + this.dir + "' is not a git repository");
    }

// Methods

    /**
     * Get the git working directory associated with this instance.
     */
    public File getDirectory() {
        return this.dir;
    }

    /**
     * Fetch data from all configured remotes to update our local copy of their information.
     *
     * @throws GitException if an error occurs
     */
    public synchronized void fetch() {
        new GitCommand("fetch", "--all").run();
    }

    /**
     * Fetch data from the named remotes to update our local copy of their information.
     *
     * @param remotes list of remotes
     * @throws GitException if an error occurs
     * @throws IllegalArgumentException if {@code remotes} or any element is null
     * @throws IllegalArgumentException if an remote does not match {@link #REMOTE_NAME_PATTERN}
     */
    public synchronized void fetch(List<String> remotes) {

        // Sanity check
        if (remotes == null)
            throw new IllegalArgumentException("null remotes");
        for (String remote : remotes) {
            if (remote == null)
                throw new IllegalArgumentException("null remote");
            if (!REMOTE_NAME_PATTERN.matcher(remote).matches())
                throw new IllegalArgumentException("illegal remote name `" + remote + "'");
        }

        // Fetch
        final ArrayList<String> params = new ArrayList<String>(remotes.size() + 2);
        params.add("fetch");
        params.add("--multiple");
        params.addAll(remotes);
        new GitCommand(params).run();
    }

    /**
     * Get the author date associated with the given commit.
     *
     * @param tree commit reference
     * @return author date
     * @throws GitException if git fails
     * @throws IllegalArgumentException if {@code tree} is null
     */
    public synchronized Date getAuthorDate(String tree) {
        try {
            return new Date(Long.parseLong(new GitCommand("log", "-1", "--format=format:%at", tree).runAndGetOutput(), 10) * 1000L);
        } catch (NumberFormatException e) {
            throw new GitException("error parsing git output", e);
        }
    }

    /**
     * Access the head of the specified branch or the named commit.
     * Any changes made by the {@link Accessor} are discarded when this method completes.
     *
     * @param branch name of the branch or commit we want to access
     * @param accessor callback interface to access the working directory contents
     * @return commit name (SHA-1 hash) corresponding to what tree was accessed
     * @throws GitException if {@code branch} is not a local branch or a commit ID
     * @throws IllegalArgumentException if {@code branch} is null
     * @throws IllegalArgumentException if {@code branch} does not match {@link #BRANCH_NAME_PATTERN}
     * @throws IllegalArgumentException if {@code branch} is null
     * @throws IllegalArgumentException if {@code accessor} is null
     */
    public synchronized String access(String branch, Accessor accessor) {

        // Sanity check
        if (branch == null)
            throw new IllegalArgumentException("null branch");
        if (!BRANCH_NAME_PATTERN.matcher(branch).matches())
            throw new IllegalArgumentException("illegal branch name `" + branch + "'");
        if (accessor == null)
            throw new IllegalArgumentException("null accessor");

        // Reset
        this.reset();

        // Checkout the commit
        new GitCommand("checkout", "--force", branch).run();

        // Get the commit ID that we have checked out
        final String commit = this.getCurrentCommit();

        // Grant access
        try {
            accessor.accessWorkingCopy(this.dir);
        } finally {
            this.reset();
        }

        // Done
        return commit;
    }

    /**
     * Commit changes onto the specified branch. The branch must already exist.
     *
     * <p>
     * If after the {@code accessor} runs nothing has changed, then no commit is performed
     * and the previous commit ID is returned.
     * </p>
     *
     * @param branch name of the branch we want to commit onto
     * @param accessor callback interface to update the working directory contents
     * @param message commit message
     * @return commit name (SHA-1 hash), or previous existing commit if no changes were made
     * @throws GitException if {@code branch} is not an existing local branch
     * @throws IllegalArgumentException if {@code branch} is null
     * @throws IllegalArgumentException if {@code branch} does not match {@link #BRANCH_NAME_PATTERN}
     * @throws IllegalArgumentException if {@code accessor} is null
     * @throws IllegalArgumentException if {@code message} is null
     */
    public synchronized String commit(String branch, Accessor accessor, String message) {

        // Sanity check
        if (branch == null)
            throw new IllegalArgumentException("null branch");
        if (!BRANCH_NAME_PATTERN.matcher(branch).matches())
            throw new IllegalArgumentException("illegal branch name `" + branch + "'");
        if (accessor == null)
            throw new IllegalArgumentException("null accessor");
        if (message == null)
            throw new IllegalArgumentException("null message");

        // Get ref file and verify branch is really a local branch
        final File refFile = this.getRepoFile("refs/heads/" + branch);
        if (!refFile.exists())
            throw new GitException("branch `" + branch + "' does not exist or is not a local branch");

        // Reset
        this.reset();

        // Perform commit steps
        try {

            // Check out branch
            new GitCommand("checkout", "--force", branch).run();

            // Apply changes
            accessor.accessWorkingCopy(this.dir);

            // Attempt commit only if something has changed
            if (new GitCommand("status", "--porcelain").runAndGetOutput().length() > 0) {

                // Stage them into the index
                new GitCommand("add", "--all").run();

                // Commit them
                new GitCommand("commit", "--message", message).run();
            }
        } finally {
            this.reset();
        }

        // Done
        return this.getCurrentCommit();
    }

    /**
     * Ensure the specified local branch exists. If it does not, it will be created with an initial empty commit.
     *
     * @param branch name of the branch we want to ensure exists
     * @param message commit message (only used if branch does not yet exist)
     * @throws IllegalArgumentException if {@code branch} is null
     * @throws IllegalArgumentException if {@code branch} does not match {@link #BRANCH_NAME_PATTERN}
     * @throws IllegalArgumentException if {@code message} is null
     */
    public synchronized void ensureBranch(String branch, String message) {

        // Sanity check
        if (branch == null)
            throw new IllegalArgumentException("null branch");
        if (!BRANCH_NAME_PATTERN.matcher(branch).matches())
            throw new IllegalArgumentException("illegal branch name `" + branch + "'");
        if (message == null)
            throw new IllegalArgumentException("null message");

        // Does branch already exist?
        final File refFile = this.getRepoFile("refs/heads/" + branch);
        if (refFile.exists())
            return;

        // Create a commit with no parents and an empty tree
        this.log.info("creating new local branch `" + branch + "' starting with an empty commit in directory `" + this.dir + "'");
        new GitCommand("read-tree", "--empty").run();
        final String tree = new GitCommand("write-tree").runAndGetOutput();
        if (!SHA1_PATTERN.matcher(tree).matches())
            throw new GitException("can't interpret output from `git write-tree': " + tree);
        final String commit = new GitCommand("commit-tree", "-m", message, tree).runAndGetOutput();

        // Create a branch starting there
        new GitCommand("branch", branch, commit).run();
    }

    /**
     * Compare two committed trees for equality.
     *
     * @param tree1 name of the first tree (branch name, commit, etc.)
     * @param tree2 name of the second tree (branch name, commit, etc.)
     * @return true if the trees are equal, otherwise false
     * @throws GitException if {@code tree1} or {@code tree2} is not a valid branch or commit reference
     * @throws IllegalArgumentException if {@code tree1} or {@code tree2} is null
     */
    public synchronized boolean equalTrees(String tree1, String tree2) {
        final GitCommand diff = new GitCommand("diff", "--quiet", tree1, tree2);
        final int exitValue = diff.run(true);
        switch (exitValue) {
        case 0:
            return true;
        case 1:
            return false;
        default:
            throw new GitException("git command " + diff.getArgs() + " in directory `" + GitRepository.this.dir
              + "' failed with exit value " + exitValue);
        }
    }

    /**
     * Merge and commit.
     *
     * <p>
     * Trivial merges, i.e., when {@code other} is already an ancestor of {@code branch}, do not result in a new commit
     * and the {@code accessor} is not run.
     * </p>
     *
     * @param branch name of the branch we want to commit onto
     * @param other name of the other commit or branch we want to merge into {@code branch}
     * @param strategy merge strategy
     * @param accessor callback interface to access the working directory contents after the merge; may be null
     * @param message commit message
     * @return commit name (SHA-1 hash)
     * @throws GitException if {@code branch} is not a local branch
     * @throws GitException if {@code other} is not a valid branch or commit reference
     * @throws GitMergeConflictException if the merge fails with conflicts
     * @throws IllegalArgumentException if {@code branch} or {@code other} is null
     * @throws IllegalArgumentException if {@code branch} does not match {@link #BRANCH_NAME_PATTERN}
     * @throws IllegalArgumentException if {@code message} is null
     */
    public synchronized String merge(String branch, String other, MergeStrategy strategy, Accessor accessor, String message) {

        // Sanity check
        if (branch == null)
            throw new IllegalArgumentException("null branch");
        if (!BRANCH_NAME_PATTERN.matcher(branch).matches())
            throw new IllegalArgumentException("illegal branch name `" + branch + "'");
        if (other == null)
            throw new IllegalArgumentException("null other");
        if (strategy == null)
            throw new IllegalArgumentException("null strategy");
        if (message == null)
            throw new IllegalArgumentException("null message");

        // Get ref file and verify branch is really a local branch
        final File refFile = this.getRepoFile("refs/heads/" + branch);
        if (!refFile.exists())
            throw new GitException("branch `" + branch + "' does not exist or is not a local branch");

        // Reset
        this.reset();

        // Perform merge steps
        try {

            // Check out branch
            new GitCommand("checkout", "--force", branch).run();

            // Attempt the merge
            final GitCommand merge = new GitCommand("merge", "--no-commit", "--no-ff");
            strategy.addOptions(merge.getArgs());
            merge.getArgs().add("--");
            merge.getArgs().add(other);
            final int exitValue = merge.run(true);
            if (exitValue != 0) {

                // Detect other types of error (other than conflicts)
                if (!this.getRepoFile("MERGE_HEAD").exists())
                    throw new GitException("git merge failed: " + merge.getStandardError().trim());

                // Abort the merge
                new GitCommand("merge", "--abort").run();

                // Bail out
                throw new GitMergeConflictException("merge failed with conflict(s)");
            }

            // Detect and ignore trivial merge
            if (new GitCommand("status", "--porcelain").runAndGetOutput().length() > 0) {

                // Grant access
                if (accessor != null)
                    accessor.accessWorkingCopy(this.dir);

                // Commit changes
                new GitCommand("commit", "--message", message).run();
            }
        } finally {

            // Clean up
            this.reset();
        }

        // Done
        return this.getCurrentCommit();
    }

    /**
     * Read file possibly containing a reference and follow it until we get a commit ID.
     *
     * @param filename filename relative to {@code .git}, e.g., {@code "HEAD"} or {@code "refs/heads/foobar"}
     * @return SHA-1 commit ID
     * @throws GitException if we can't figure it out
     */
    public String followReference(String filename) {
        for (int i = 0; i < 10; i++) {

            // Read file
            final File file = this.getRepoFile(filename);
            String line;
            try {
                line = this.readFirstLine(file);
            } catch (IOException e) {
                throw new GitException("error reading `" + file + "'", e);
            }

            // Is it a SHA-1?
            if (SHA1_PATTERN.matcher(line).matches())
                return line;

            // Is it a reference?
            final Matcher matcher = Pattern.compile("ref: (.*)$").matcher(line);
            if (matcher.matches()) {
                filename = matcher.group(1);
                continue;
            }

            // Dunno
            throw new GitException("can't interpret contents of `" + file + "': " + line);
        }
        throw new GitException("too many levels of git references");
    }

    /**
     * Clean out and reset working directory.
     */
    protected void reset() {
        new GitCommand("clean", "-xdf").run();                      // clean out working directory
        new GitCommand("reset", "--hard").run();                    // discard uncommitted changes
    }

    /**
     * Get the commit ID of what we have currently checked out (i.e., our HEAD).
     *
     * @throws GitException if we can't figure it out
     */
    protected String getCurrentCommit() {
        return this.followReference("HEAD");
    }

    /**
     * Read the first line of a file (trimmed) as a {@link String}.
     */
    protected String readFirstLine(File file) throws IOException {
        final FileInputStream inputStream = new FileInputStream(file);
        try {
            final LineNumberReader reader = new LineNumberReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")));
            final String firstLine = reader.readLine();
            if (firstLine == null)
                throw new GitException("read empty content from `" + file + "'");
            return firstLine.trim();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    /**
     * Get the {@link File} corresponding to the given relative path under the {@code .git} directory.
     *
     * @param path relative path, e.g., {@code HEAD} or {@code refs/heads/foo}
     */
    public File getRepoFile(String path) {
        return new File(new File(this.dir, ".git"), path);
    }

// Accessor

    /**
     * Callback interface used to access or modify working directory contents.
     */
    public interface Accessor {

        /**
         * Access the working copy in the specified working directory.
         *
         * @param dir working directory root
         * @throws IOException if an I/O error occurs
         */
        void accessWorkingCopy(File dir);
    }

// GitCommand

    /**
     * A simple wrapper around the {@code git(1)} command.
     */
    public class GitCommand {

        private final Logger log = LoggerFactory.getLogger(this.getClass());
        private final List<String> args = new ArrayList<String>();

        private ProcessRunner runner;

        /**
         * Constructor.
         *
         * @param params the command line arguments (not including the initial {@code "git"})
         */
        public GitCommand(String... params) {
            this(Arrays.asList(params));
        }

        /**
         * Constructor.
         *
         * @param params the command line arguments (not including the initial {@code "git"})
         */
        public GitCommand(List<String> params) {
            if (params == null)
                throw new IllegalArgumentException("null params");
            this.args.add("git");
            this.args.addAll(params);
        }

        /**
         * Get command line arguments.
         */
        public List<String> getArgs() {
            return this.args;
        }

        /**
         * Run command. Equivalent to {@code run(false)}.
         */
        public int run() {
            return this.run(false);
        }

        /**
         * Run command.
         *
         * @param errorOk true if a non-zero exit value may be expected
         * @return {@code git(1)} exit value; if {@code errorOk} is false this will always be zero
         * @throws IllegalStateException if this command has already been run
         * @throws RuntimeException if the current thread is interrupted
         * @throws GitException if any {@code GIT_*} environment variables are set
         * @throws GitException if {@code errorOk} is false and {@code git(1}} exits with a non-zero value
         */
        public int run(boolean errorOk) {

            // Sanity check
            if (this.runner != null)
                throw new IllegalStateException("command already executed");

            // Sanity check environment
            final ArrayList<String> vars = new ArrayList<String>();
            for (String var : System.getenv().keySet()) {
                if (var.startsWith("GIT_"))
                    vars.add(var);
            }
            if (!vars.isEmpty())
                throw new GitException("need to unset GIT_* environment variables first: " + vars);

            // Start process
            Process process;
            try {
                process = Runtime.getRuntime().exec(args.toArray(new String[args.size()]), null, GitRepository.this.dir);
            } catch (IOException e) {
                this.log.debug("git command " + this.args + " in directory `" + GitRepository.this.dir + "' failed: ", e);
                throw new GitException("error invoking git(1)", e);
            }
            this.runner = new ProcessRunner(process);

            // Let it finish
            int exitValue;
            try {
                exitValue = this.runner.run();
            } catch (InterruptedException e) {
                throw new RuntimeException("unexpected exception", e);      // XXX
            }

            // Log command output
            final StringBuilder buf = new StringBuilder();
            final String[] stdout = this.getStandardOutput().trim().split("\n");
            if (!(stdout.length == 1 && stdout[0].length() == 0)) {
                for (String line : stdout)
                    buf.append("\n  [stdout] ").append(line);
            }
            final String[] stderr = this.getStandardError().trim().split("\n");
            if (!(stderr.length == 1 && stderr[0].length() == 0)) {
                for (String line : stderr)
                    buf.append("\n  [stderr] ").append(line);
            }

            // Check exit value
            if (exitValue != 0) {
                final String msg = "git command " + this.args + " in directory `" + GitRepository.this.dir + "' failed";
                if (errorOk)
                    this.log.debug(msg);
                else {
                    this.log.error(msg);
                    throw new GitException(msg);
                }
            } else {
                this.log.debug("successfully executed git command " + this.args + " in directory `"
                  + GitRepository.this.dir + "'" + buf);
            }

            // Done
            return exitValue;
        }

        /**
         * Run command and return standard output, interpreted as a UTF-8 string, with leading and trailing whitespace trimmed.
         *
         * @throws IllegalStateException if this command has already been run
         * @throws RuntimeException if the current thread is interrupted
         * @throws GitException if any {@code GIT_*} environment variables are set
         * @throws GitException if {@code git(1}} exits with a non-zero value
         */
        public String runAndGetOutput() {
            this.run();
            return this.getStandardOutput().trim();
        }

        /**
         * Get the standard output from {@code git(1)}.
         *
         * @throws IllegalStateException if command has not yet completed
         * @return command standard output interpreted as UTF-8
         */
        public String getStandardOutput() {
            if (this.runner == null)
                throw new IllegalStateException("command has not yet executed");
            return new String(this.runner.getStandardOutput(), Charset.forName("UTF-8"));
        }

        /**
         * Get the standard error from {@code git(1)}.
         *
         * @throws IllegalStateException if command has not yet completed
         * @return command standard error interpreted as UTF-8
         */
        public String getStandardError() {
            if (this.runner == null)
                throw new IllegalStateException("command has not yet executed");
            return new String(this.runner.getStandardError(), Charset.forName("UTF-8"));
        }
    }
}

