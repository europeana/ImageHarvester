package eu.europeana.harvester.util.pagedElements;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import eu.europeana.harvester.domain.Page;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Alexandru Stefanica, alexandru.stefanica@busymachines.com
 * @since 03 Mar 2016
 */
public abstract class PagedElements<T> implements Iterable<List<T>> {
    private final DBCursor dbCursor_;
    private final int pageSize_;

    public PagedElements (DBCursor dbCursor, int pageSize) {
        dbCursor_ = dbCursor;
        pageSize_ = pageSize;
    }

    public int getPageSize() {return pageSize_;}

    public boolean hasNext() {return dbCursor_.hasNext();}

    public List<T> getNextPage() {
        final List<T> elements = new ArrayList<>(pageSize_);
        for (int i = 0; i < pageSize_ && dbCursor_.hasNext(); ++i) {
            elements.add(extractFromDBObject((BasicDBObject)dbCursor_.next()));
        }

        return elements;
    }

    @Override
    public Iterator<List<T>> iterator() {
        return new Iterator<List<T>>(){

            @Override
            public boolean hasNext() {
                return PagedElements.this.hasNext();
            }

            @Override
            public List<T> next() {return getNextPage();}

            @Override
            public void remove() {
                throw new NotImplementedException();
            }
        };
    }

    abstract protected T extractFromDBObject(BasicDBObject o);
}
