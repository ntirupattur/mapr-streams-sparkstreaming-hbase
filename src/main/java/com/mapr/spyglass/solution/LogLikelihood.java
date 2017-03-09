package com.mapr.spyglass.solution;

import com.google.common.base.Preconditions;

/**
 * Utility methods for working with log-likelihood
 */
public final class LogLikelihood {

    private LogLikelihood() {
    }

    /**
     * Calculates the unnormalized Shannon entropy.  This is
     *
     * -sum x_i log x_i / N = -N sum x_i/N log x_i/N
     *
     * where N = sum x_i
     *
     * If the x's sum to 1, then this is the same as the normal
     * expression.  Leaving this un-normalized makes working with
     * counts and computing the LLR easier.
     *
     * @return The entropy value for the elements
     */
    public static double entropy(long... elements) {
        long sum = 0;
        double result = 0.0;
        for (long element : elements) {
            Preconditions.checkArgument(element >= 0);
            result += xLogX(element);
            sum += element;
        }
        return xLogX(sum) - result;
    }

    private static double xLogX(long x) {
        return x == 0 ? 0.0 : x * Math.log(x);
    }

    /**
     * Merely an optimization for the common two argument case of {@link #entropy(long...)}
     * @see #logLikelihoodRatio(long, long, long, long)
     */
    private static double entropy(long a, long b) {
        return xLogX(a + b) - xLogX(a) - xLogX(b);
    }

    /**
     * Merely an optimization for the common four argument case of {@link #entropy(long...)}
     * @see #logLikelihoodRatio(long, long, long, long)
     */
    private static double entropy(long a, long b, long c, long d) {
        return xLogX(a + b + c + d) - xLogX(a) - xLogX(b) - xLogX(c) - xLogX(d);
    }

    /**
     * Calculates the Raw Log-likelihood ratio for two events, call them A and B.  Then we have:
     * <p/>
     * <table border="1" cellpadding="5" cellspacing="0">
     * <tbody><tr><td>&nbsp;</td><td>Event A</td><td>Everything but A</td></tr>
     * <tr><td>Event B</td><td>A and B together (k_11)</td><td>B, but not A (k_12)</td></tr>
     * <tr><td>Everything but B</td><td>A without B (k_21)</td><td>Neither A nor B (k_22)</td></tr></tbody>
     * </table>
     *
     * @param k11 The number of times the two events occurred together
     * @param k12 The number of times the second event occurred WITHOUT the first event
     * @param k21 The number of times the first event occurred WITHOUT the second event
     * @param k22 The number of times something else occurred (i.e. was neither of these events
     * @return The raw log-likelihood ratio
     *
     * <p/>
     * Credit to http://tdunning.blogspot.com/2008/03/surprise-and-coincidence.html for the table and the descriptions.
     */
    public static double logLikelihoodRatio(long k11, long k12, long k21, long k22) {
        Preconditions.checkArgument(k11 >= 0 && k12 >= 0 && k21 >= 0 && k22 >= 0);
        // note that we have counts here, not probabilities, and that the entropy is not normalized.
        double rowEntropy = entropy(k11 + k12, k21 + k22);
        double columnEntropy = entropy(k11 + k21, k12 + k22);
        double matrixEntropy = entropy(k11, k12, k21, k22);
        if (rowEntropy + columnEntropy < matrixEntropy) {
            // round off error
            return 0.0;
        }
        return 2.0 * (rowEntropy + columnEntropy - matrixEntropy);
    }

    /**
     * Calculates the root log-likelihood ratio for two events.
     * See {@link #logLikelihoodRatio(long, long, long, long)}.

     * @param k11 The number of times the two events occurred together
     * @param k12 The number of times the second event occurred WITHOUT the first event
     * @param k21 The number of times the first event occurred WITHOUT the second event
     * @param k22 The number of times something else occurred (i.e. was neither of these events
     * @return The root log-likelihood ratio
     *
     * <p/>
     * There is some more discussion here: http://s.apache.org/CGL
     *
     * And see the response to Wataru's comment here:
     * http://tdunning.blogspot.com/2008/03/surprise-and-coincidence.html
     */
    public static double rootLogLikelihoodRatio(long k11, long k12, long k21, long k22) {
        double llr = logLikelihoodRatio(k11, k12, k21, k22);
        double sqrt = Math.sqrt(llr);
        if ((double) k11 / (k11 + k12) < (double) k21 / (k21 + k22)) {
            sqrt = -sqrt;
        }
        return sqrt;
    }
}