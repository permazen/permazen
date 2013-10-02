
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.util;

import java.io.IOException;
import java.io.Writer;

import org.apache.log4j.WriterAppender;

/**
 * A Log4J appender that can be configured on a per-thread basis to direct the log messages
 * to an arbitrary {@link Writer} destination.
 *
 * <p>
 * Normally this appender does nothing. However, if a thread invokes {@link #setLogDestination setLogDestination()}
 * with a non-null {@link Writer}, then any messages logged by <i>that particular thread</i> will go to that {@link Writer}.
 * This allows individual threads to copy and/or redirect their own log messages as they see fit.
 * </p>
 *
 * <p>
 * Configure this appender just as you would any other. For example:
 *
 *  <blockquote><pre>
 *  &lt;!-- Per-thread logger --&gt;
 *  &lt;appender name="threadlog" class="org.dellroad.stuff.util.ThreadLogger"&gt;
 *      &lt;param name="Threshold" value="debug"/&gt;
 *      &lt;layout class="org.apache.log4j.PatternLayout"&gt;
 *          &lt;param name="ConversionPattern" value="%d{ISO8601} %p: %m%n"/&gt;
 *      &lt;/layout&gt;
 *  &lt;/appender&gt;
 *
 *  ...
 *
 *  &lt;root&gt;
 *      ...
 *      &lt;appender-ref ref="threadlog"&gt;
 *  &lt;/root&gt;
 *  </pre></blockquote>
 * </p>
 */
public class ThreadLogger extends WriterAppender {

    private static final InheritableThreadLocal<WriterInfo> CURRENT_WRITER = new InheritableThreadLocal<WriterInfo>() {
        @Override
        protected WriterInfo childValue(WriterInfo writerInfo) {
            return writerInfo != null && writerInfo.isInherit() ? writerInfo : null;
        }
    };

    public ThreadLogger() {
        this.setWriter(new ThreadWriter());
    }

    /**
     * Configure the logging output destination for the current thread.
     * Optionally also applies to all of the current thread's descendant threads.
     *
     * @param writer current thread logging destination, or null for none
     * @param inherit whether {@code writer} (if not null) should be inherited by descendant threads
     */
    public static void setLogDestination(Writer writer, boolean inherit) {
        CURRENT_WRITER.set(writer != null ? new WriterInfo(writer, inherit) : null);
    }

    /**
     * Configure the logging output destination for the current thread and all its descendant threads.
     * This is a convenience method, equivalent to
     * {@link #setLogDestination(Writer, boolean) setLogDestination}{@code (writer, true)}.
     *
     * @param writer current thread logging destination, or null for none
     */
    public static void setLogDestination(Writer writer) {
        ThreadLogger.setLogDestination(writer, true);
    }

    /**
     * Get the currently configured logging output destination for the current thread.
     *
     * @return current thread logging destination, or null if there is none
     */
    public static Writer getLogDestination() {
        final WriterInfo writerInfo = CURRENT_WRITER.get();
        return writerInfo != null ? writerInfo.getWriter() : null;
    }

    @Override
    protected boolean checkEntryConditions() {
        return CURRENT_WRITER.get() != null && super.checkEntryConditions();
    }

    // Per-thread info
    private static class WriterInfo {

        private final Writer writer;
        private final boolean inherit;

        WriterInfo(Writer writer, boolean inherit) {
            this.writer = writer;
            this.inherit = inherit;
        }

        public Writer getWriter() {
            return this.writer;
        }

        public boolean isInherit() {
            return this.inherit;
        }
    }

    // Wrapper writer
    private static class ThreadWriter extends Writer {

        @Override
        public void write(char[] buf, int off, int len) throws IOException {
            final WriterInfo writerInfo = CURRENT_WRITER.get();
            if (writerInfo != null)
                writerInfo.getWriter().write(buf, off, len);
        }

        @Override
        public void close() throws IOException {
            final WriterInfo writerInfo = CURRENT_WRITER.get();
            if (writerInfo != null)
                writerInfo.getWriter().close();
        }

        @Override
        public void flush() throws IOException {
            final WriterInfo writerInfo = CURRENT_WRITER.get();
            if (writerInfo != null)
                writerInfo.getWriter().flush();
        }
    }
}

