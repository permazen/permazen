
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;

import java.io.Serializable;

/**
 * Non-null {@link InternetAddress} encoding.  Null values are not supported by this class.
 *
 * <p>
 * <b>Note:</b> the method {@link InternetAddress#equals InternetAddress.equals()} performs a case-insensitive
 * comparison of email addresses, ignoring the personal name, whereas this encoding distinguishes instances
 * that are not exactly equal.
 */
public class InternetAddressEncoding extends StringConvertedEncoding<InternetAddress> {

    private static final long serialVersionUID = 289940859247032224L;

    public InternetAddressEncoding() {
        super(EncodingIds.builtin("InternetAddress"), InternetAddress.class, new InternetAddressConverter());
    }

// EmailConverter

    private static class InternetAddressConverter extends Converter<InternetAddress, String> implements Serializable {

        private static final long serialVersionUID = 3837763387234872160L;

        @Override
        protected String doForward(InternetAddress address) {
            if (address == null)
                return null;
            return address.toString();
        }

        @Override
        protected InternetAddress doBackward(String string) {
            if (string == null)
                return null;
            try {
                return new InternetAddress(string);
            } catch (AddressException e) {
                throw new IllegalArgumentException("invalid email address \"" + string + "\"", e);
            }
        }
    }
}
