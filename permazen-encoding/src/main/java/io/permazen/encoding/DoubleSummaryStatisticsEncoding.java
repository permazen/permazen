
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import java.util.DoubleSummaryStatistics;

/**
 * Non-null {@link DoubleSummaryStatistics} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Instances are ordered by sum, count, min, max.
 */
public class DoubleSummaryStatisticsEncoding extends Concat4Encoding<DoubleSummaryStatistics, Double, Long, Double, Double> {

    private static final long serialVersionUID = -1637830934776137662L;

    public DoubleSummaryStatisticsEncoding() {
        super(DoubleSummaryStatistics.class,
          new DoubleEncoding(null),
          new LongEncoding(null),
          new DoubleEncoding(null),
          new DoubleEncoding(null),
          DoubleSummaryStatistics::getSum,
          DoubleSummaryStatistics::getCount,
          DoubleSummaryStatistics::getMin,
          DoubleSummaryStatistics::getMax,
          tuple -> new DoubleSummaryStatistics(tuple.getValue2(), tuple.getValue3(), tuple.getValue4(), tuple.getValue1()));
    }
}
