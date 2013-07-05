
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.servlet;

import java.io.IOException;
import java.net.SocketException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Servlet filter that ensures any exceptions which are not handled by the servlet get logged somewhere.
 */
public class ExceptionLoggingFilter extends OncePerRequestFilter {

    private final Logger defaultLogger = LoggerFactory.getLogger(this.getClass());

    /**
     * Process the request. If an exception is thrown, it will be (possibly) logged and re-thrown.
     *
     * @see #shouldLogException
     */
    @Override
    public void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {
        try {
            filterChain.doFilter(request, response);
        } catch (IOException e) {
            if (this.shouldLogException(e))
                this.logException(e);
            throw e;
        } catch (ServletException e) {
            if (this.shouldLogException(e))
                this.logException(e);
            throw e;
        } catch (RuntimeException e) {
            if (this.shouldLogException(e))
                this.logException(e);
            throw e;
        } catch (Error e) {
            if (this.shouldLogException(e))
                this.logException(e);
            throw e;
        } catch (Throwable e) {
            if (this.shouldLogException(e))
                this.logException(e);
           throw new ServletException(e);
        }
    }

    /**
     * Determine if an exception caught by this instance should be logged.
     *
     * <p>
     * The implementation in {@link ExceptionLoggingFilter} returns {@code true} except for
     * {@link SocketException} (typically caused by the client disconnecting) and {@link ThreadDeath}
     * (typically caused by virtual machine shutdown).
     * Subclasses should override if necessary.
     *
     * @param t exception caught by this instance
     */
    protected boolean shouldLogException(Throwable t) {
        if (t instanceof SocketException)
            return false;
        if (t instanceof ThreadDeath)
            return false;
        return true;
    }

    /**
     * Log an exception caught by this instance.
     *
     * <p>
     * The implementation in {@link ExceptionLoggingFilter} logs the exception as an error via the logger
     * returned by {@link #getLogger getLogger()}.
     * Subclasses should override if necessary.
     *
     * @param t exception caught by this instance
     */
    protected void logException(Throwable t) {
        this.getLogger(t).error("exception within servlet", t);
    }

    /**
     * Get the logging destination.
     *
     * <p>
     * The implementation in {@link ExceptionLoggingFilter} uses the {@link Logger} returned by
     * {@link LoggerFactory#getLogger} when passed this instance's class as the parameter.
     * Subclasses should override if necessary.
     *
     * @param t the exception about to be logged
     */
    protected Logger getLogger(Throwable t) {
        return this.defaultLogger;
    }
}

