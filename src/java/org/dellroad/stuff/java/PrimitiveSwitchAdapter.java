
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

/**
 * Adapter class for {@link PrimitiveSwitch} implementations.
 *
 * @param <R> switch method return type
 * @since 1.0.65
 */
public class PrimitiveSwitchAdapter<R> implements PrimitiveSwitch<R> {

    /**
     * Handle the {@link Primitive#VOID} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseDefault}.
     */
    @Override
    public R caseVoid() {
        return caseDefault();
    }

    /**
     * Handle the {@link Primitive#BOOLEAN} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseDefault}.
     */
    @Override
    public R caseBoolean() {
        return caseDefault();
    }

    /**
     * Handle the {@link Primitive#BYTE} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseNumber}.
     */
    @Override
    public R caseByte() {
        return caseNumber();
    }

    /**
     * Handle the {@link Primitive#CHARACTER} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseDefault}.
     */
    @Override
    public R caseCharacter() {
        return caseDefault();
    }

    /**
     * Handle the {@link Primitive#SHORT} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseNumber}.
     */
    @Override
    public R caseShort() {
        return caseNumber();
    }

    /**
     * Handle the {@link Primitive#INTEGER} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseNumber}.
     */
    @Override
    public R caseInteger() {
        return caseNumber();
    }

    /**
     * Handle the {@link Primitive#FLOAT} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseNumber}.
     */
    @Override
    public R caseFloat() {
        return caseNumber();
    }

    /**
     * Handle the {@link Primitive#LONG} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseNumber}.
     */
    @Override
    public R caseLong() {
        return caseNumber();
    }

    /**
     * Handle the {@link Primitive#DOUBLE} case.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseNumber}.
     */
    @Override
    public R caseDouble() {
        return caseNumber();
    }

    /**
     * Internal roll-up method.
     * The implementation in {@link PrimitiveSwitchAdapter} delegates to {@link #caseDefault}.
     */
    protected R caseNumber() {
        return caseDefault();
    }

    /**
     * Internal roll-up method.
     * The implementation in {@link PrimitiveSwitchAdapter} returns {@code null}.
     */
    protected R caseDefault() {
        return null;
    }
}

