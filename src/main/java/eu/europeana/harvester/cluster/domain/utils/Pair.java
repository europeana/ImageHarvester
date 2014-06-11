package eu.europeana.harvester.cluster.domain.utils;

/**
 * A pair consisting of two elements.
 * @param <E1> the left element type
 * @param <E2> the right element type
 */
public class Pair<E1, E2> {

    private E1 e1;

    private E2 e2;

    public Pair(E1 e1, E2 e2) {
        this.e1 = e1;
        this.e2 = e2;
    }

    public E1 getKey() {
        return e1;
    }

    public E2 getValue() {
        return e2;
    }

}
