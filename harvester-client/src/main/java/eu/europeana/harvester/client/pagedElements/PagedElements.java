package eu.europeana.harvester.client.pagedElements;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Alexandru Stefanica, alexandru.stefanica@busymachines.com
 * @since 03 Mar 2016
 */
public abstract class PagedElements<T> implements Iterable<List<T>> {
    private final DBCursor dbCurosr_;
    private final int pageSize_;

    public PagedElements (DBCursor dbCursor, int pageSize) {
        dbCurosr_ = dbCursor;
        pageSize_ = pageSize;
    }

    public int getPageSize() {return pageSize_;}

    @Override
    public Iterator<List<T>> iterator() {
        return new Iterator<List<T>>(){

            @Override
            public boolean hasNext() {
                return dbCurosr_.hasNext();
            }

            @Override
            public List<T> next() {
                final List<T> elements = new ArrayList<>(pageSize_);
                for (int i = 0; i < pageSize_ && dbCurosr_.hasNext(); ++i) {
                    elements.add(extractFromDBObject((BasicDBObject)dbCurosr_.next()));
                }

                return elements;
            }

            @Override
            public void remove() {
                throw new NotImplementedException();
            }
        };
    }

    abstract protected T extractFromDBObject(BasicDBObject o);
}
