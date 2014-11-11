
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.distrib;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.dellroad.stuff.java.ProcessRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple wrapper around the {@code git(1)} command.
 */
public class GitCommand {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private final File dir;
    private final List<String> args = new ArrayList<String>();

    private ProcessRunner runner;

    /**
     * Constructor.
     *
     * @param dir repository filesystem location (i.e., the directory containing {@code .git} as a subdirectory)
     * @param params the command line arguments (not including the initial {@code "git"})
     */
    public GitCommand(File dir, String... params) {
        this(dir, Arrays.asList(params));
    }

    /**
     * Constructor.
     *
     * @param dir repository filesystem location (i.e., the directory containing {@code .git} as a subdirectory)
     * @param params the command line arguments (not including the initial {@code "git"})
     * @throws IllegalArgumentException if {@code dir} is null
     */
    public GitCommand(File dir, List<String> params) {
        if (dir == null)
            throw new IllegalArgumentException("null dir");
        if (params == null)
            throw new IllegalArgumentException("null params");
        this.dir = dir;
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
            process = Runtime.getRuntime().exec(args.toArray(new String[args.size()]), null, this.dir);
        } catch (IOException e) {
            this.log.debug("command `" + this + "' in directory `" + this.dir + "' failed: ", e);
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
            final String msg = "command `" + this + "' in directory `" + this.dir + "' failed with exit value " + exitValue;
            if (errorOk)
                this.log.debug(msg);
            else {
                this.log.error(msg);
                throw new GitException(msg);
            }
        } else
            this.log.debug("successfully executed command `" + this + "' in directory `" + this.dir + "'" + buf);

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

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        for (String arg : this.args) {
            if (buf.length() > 0)
                buf.append(' ');
            buf.append(arg);
        }
        return buf.toString();
    }
}

