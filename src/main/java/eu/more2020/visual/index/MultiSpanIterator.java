package eu.more2020.visual.index;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class MultiSpanIterator<T> implements Iterator<T> {

    private final Iterator<Iterator<T>> chain;
    private Iterator<T> currentIterator;
    private Iterator<T> lastIterator;


    public MultiSpanIterator(Iterator<Iterable<T>> iterator)  {
        List<Iterator<T>> iteratorList = new ArrayList<>();
        for (Iterator<Iterable<T>> it = iterator; it.hasNext(); ) {
            Iterable iterable = it.next();
            Iterator<T> iterator1 = iterable.iterator();
            iteratorList.add(iterator1);
        }
        this.chain = iteratorList.iterator();
    }


    @Override
    public boolean hasNext() {
        while (currentIterator == null || !currentIterator.hasNext()) {
            if (!chain.hasNext()) return false;
            currentIterator = chain.next();
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
}
