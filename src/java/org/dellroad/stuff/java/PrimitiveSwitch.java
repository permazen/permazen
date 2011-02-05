
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

/**
 * Visitor pattern interface for {@link Primitive}s.
 *
 * @param <R> switch method return type
 */
public interface PrimitiveSwitch<R> {

    R caseBoolean();

    R caseByte();

    R caseCharacter();

    R caseShort();

    R caseInteger();

    R caseFloat();

    R caseLong();

    R caseDouble();
}

