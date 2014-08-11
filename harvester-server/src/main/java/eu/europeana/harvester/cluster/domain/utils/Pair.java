package eu.europeana.harvester.cluster.domain.utils;

import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * A pair consisting of two elements.
 * @param <E1> the left element type
 * @param <E2> the right element type
 */
public class Pair<E1, E2> {

    private final E1 e1;

    private final E2 e2;

    public Pair(final E1 e1, final E2 e2) {
        this.e1 = e1;
        this.e2 = e2;
    }

    public E1 getKey() {
        return e1;
    }

    public E2 getValue() {
        return e2;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
                // if deriving: appendSuper(super.hashCode()).
                append(e1).
                append(e2).
                toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof Pair) {
            if(this.e1.equals(((Pair) obj).e1) && this.e2.equals(((Pair) obj).e2)) {
                return true;
            }
        }

        return false;
    }
}
