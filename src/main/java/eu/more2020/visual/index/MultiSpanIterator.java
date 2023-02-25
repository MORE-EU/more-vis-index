package eu.more2020.visual.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MultiSpanIterator<T> implements Iterator<T> {

    private final Iterator<Iterator<T>> iteratorChain;
    private final Iterator<Iterable<T>> iterableChain;

    private final List<Iterable<T>> iterables;
    private Iterator<T> currentIterator;
    private Iterable<T> currentIterable;

    private Iterator<T> lastIterator;



    public MultiSpanIterator(Iterator<Iterable<T>> iterator)  {
        List<Iterator<T>> iteratorList = new ArrayList<>();
        List<Iterable<T>> iterablesList = new ArrayList<>();
        for (Iterator<Iterable<T>> it = iterator; it.hasNext(); ) {
            Iterable iterable = it.next();
            Iterator<T> iterator1 = iterable.iterator();
            iterablesList.add(iterable);
            iteratorList.add(iterator1);
        }
        this.iterables = iterablesList;
        this.iteratorChain = iteratorList.iterator();
        this.iterableChain = iterablesList.iterator();
    }


    @Override
    public boolean hasNext() {
        while (currentIterator == null || !currentIterator.hasNext()) {
            if (!iteratorChain.hasNext()) return false;
            currentIterator = iteratorChain.next();
            currentIterable = iterableChain.next();
        }
        return true;
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            this.lastIterator = null;         // to disallow remove()
            throw new NoSuchElementException();
        }
        this.lastIterator = currentIterator;  // to support remove()
        return currentIterator.next();
    }


    @Override
    public void remove() {
        if (this.lastIterator == null) {
            throw new IllegalStateException();
        }
        this.lastIterator.remove();
    }

    public Iterator<T> getCurrentIterator() {
        return currentIterator;
    }

    public Iterable<T> getCurrentIterable() {
        return currentIterable;
    }
}
