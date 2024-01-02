
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Logging callback interface used during a {@link Jsck} key/value database consistency inspection.
 */
public interface JsckLogger {

    /**
     * Log a low-detail (informational) message.
     *
     * @param format {@link String#format String.format()} format string
     * @param args format arguments
     */
    void info(String format, Object... args);

    /**
     * Log a high-detail message.
     *
     * @param format {@link String#format String.format()} format string
     * @param args format arguments
     */
    void detail(String format, Object... args);

    /**
     * Determine whether detailed logging should be performed.
     *
     * <p>
     * When this returns true, detailed logging is delivered to {@link #detail detail()},
     * otherwise it is suppressed.
     *
     * @return true if detailed logging should be performed
     */
    boolean isDetailEnabled();

    /**
     * Create an instance that wraps the given {@link Logger} and logs at {@link Level#INFO} and {@link Level#TRACE}.
     *
     * @param logger destination for log messages
     * @return {@link JsckLogger} that logs to {@code logger}
     * @throws IllegalArgumentException if {@code logger} is null
     */
    static JsckLogger wrap(final Logger logger) {
        Preconditions.checkArgument(logger != null);
        return JsckLogger.wrap(logger, logger.isInfoEnabled() ? Level.INFO : null, logger.isTraceEnabled() ? Level.TRACE : null);
    }

    /**
     * Create an instance that wraps the given {@link Logger}.
     *
     * <p>
     * The returned instance logs {@link #info info()} messages at {@code infoLevel} and
     * and {@link #detail detail()} messages to at {@code detailLevel}.
     *
     * @param logger destination for log messages
     * @param infoLevel log level for {@link #info info()} messages, or null to suppress info messages
     * @param detailLevel log level for {@link #detail detail()} messages, or null to suppress detail messages
     * @return {@link JsckLogger} that logs to {@code logger}
     * @throws IllegalArgumentException if {@code logger} is null
     */
    static JsckLogger wrap(final Logger logger, final Level infoLevel, final Level detailLevel) {
        Preconditions.checkArgument(logger != null);
        return new JsckLogger() {

            @Override
            public boolean isDetailEnabled() {
                return this.isEnabled(detailLevel);
            }

            @Override
            public void info(String format, Object... args) {
                this.log(infoLevel, format, args);
            }

            @Override
            public void detail(String format, Object... args) {
                this.log(detailLevel, format, args);
            }

            private void log(Level level, String format, Object... args) {
                if (!this.isEnabled(level))
                    return;
                switch (level) {
                case TRACE:
                    logger.trace("{}", String.format(format, args));
                    break;
                case DEBUG:
                    logger.debug("{}", String.format(format, args));
                    break;
                case INFO:
                    logger.info("{}", String.format(format, args));
                    break;
                case WARN:
                    logger.warn("{}", String.format(format, args));
                    break;
                case ERROR:
                    logger.error("{}", String.format(format, args));
                    break;
                default:
                    break;
                }
            }

            private boolean isEnabled(Level level) {
                if (level == null)
                    return false;
                switch (level) {
                case TRACE:
                    return logger.isTraceEnabled();
                case DEBUG:
                    return logger.isDebugEnabled();
                case INFO:
                    return logger.isInfoEnabled();
                case WARN:
                    return logger.isWarnEnabled();
                case ERROR:
                    return logger.isErrorEnabled();
                default:
                    return false;
                }
            }
        };
    }
}
