/*
 * @(#) TransformingList.java
 * Created Jun 16, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.thrift;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.apache.commons.collections.CollectionUtils;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public class TransformingList<SourceElement,TargetElement> implements List<TargetElement> 
{
    private final List<SourceElement> source;
    private final Transformer<SourceElement, TargetElement> transformer;
    private List<TargetElement> translated;

    /**
     * @param source
     * @param transformer
     */
    public TransformingList(List<SourceElement> source,
            Transformer<SourceElement, TargetElement> transformer)
    {
        this.source = source;
        this.transformer = transformer;
    }
    
    public int size()
    {
        return source.size();
    }
    
    public boolean isEmpty()
    {
        return source.isEmpty();
    }
    public boolean contains(Object o)
    {
        return source.contains(o);
    }
    
    public Iterator<TargetElement> iterator()
    {
        return new Iterator<TargetElement>()
        {
            private final Iterator<SourceElement> sourceIterator= source.iterator();

            @Override
            public boolean hasNext()
            {
                return sourceIterator.hasNext();
            }

            @Override
            public TargetElement next()
            {
                return transformer.transform(sourceIterator.next());
            }

            @Override
            public void remove()
            {
                sourceIterator.remove();
            }
            
        }; 
    }
    
    public Object[] toArray()
    {
        throw new UnsupportedOperationException(
        "Method TransformingList.toArray() is not supported, because it is not efficient");
    }
    
    public <T> T[] toArray(T[] a)
    {
        throw new UnsupportedOperationException(
        "Method TransformingList.toArray() is not supported, because it is not efficient");
    }
    
    public boolean add(TargetElement e)
    {
        throw new UnsupportedOperationException(
                "Method TransformingList.add(e) is not supported, because list is readonly");
//        return source.add(e);
    }
    
    public boolean remove(Object o)
    {
        throw new UnsupportedOperationException(
                "Method TransformingList.remove(o) is not supported, because list is readonly");
    }
    
    private List<TargetElement> translateAll()
    {
        if (translated!=null)
            return translated;
        
        return translated=new ArrayList<TargetElement>(this);
    }

    public boolean containsAll(Collection<?> c)
    {
        return translateAll().containsAll(c);
    }
    
    public boolean addAll(Collection<? extends TargetElement> c)
    {
        throw new UnsupportedOperationException(
            "Method TransformingList.addAll(c) is not supported, list is readonly");        
    }

    public boolean addAll(int index, Collection<? extends TargetElement> c)
    {
        throw new UnsupportedOperationException(
        "Method TransformingList.addAll(c) is not supported, list is readonly");        
//        return source.addAll(index, c);
    }
    public boolean removeAll(Collection<?> c)
    {
        throw new UnsupportedOperationException(
        "Method TransformingList.removeAll(c) is not supported, list is readonly");        
//        return source.removeAll(c);
    }
    
    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException(
                "Method TransformingList.retainAll(c) is not supported, list is readonly");
//        return source.retainAll(c);
    }
    
    public void clear()
    {
        source.clear();
        translated=null;
    }
    
    public TargetElement get(int index)
    {
        return transformer.transform(source.get(index));
    }
    
    public TargetElement set(int index, TargetElement element)
    {
        throw new UnsupportedOperationException(
                "Method TransformingList.set(index, element) is not supported");
        
//        return source.set(index, element);
    }
    public void add(int index, TargetElement element)
    {
        throw new UnsupportedOperationException(
                "Method TransformingList.add(index, element) is not supported");
//        source.add(index, element);
    }
    public TargetElement remove(int index)
    {
        translated=null;

        return transformer.transform(source.remove(index));
    }
    
    public int indexOf(Object o)
    {
        return translateAll().indexOf(o);
    }
    
    public int lastIndexOf(Object o)
    {
        return translateAll().lastIndexOf(o);
    }
    
    public ListIterator<TargetElement> listIterator()
    {
        return new ListIteratorImplementation(source.listIterator());
        
    }
    
    public ListIterator<TargetElement> listIterator(int index)
    {
        return new ListIteratorImplementation(source.listIterator(index));
    }
    
    public List<TargetElement> subList(int fromIndex, int toIndex)
    {
        return new TransformingList<SourceElement, TargetElement>(source.subList(fromIndex, toIndex),transformer);
    }

    /**
     * @author Oleg Anastasyev<oa@hq.one.lv>
     *
     */
    private final class ListIteratorImplementation implements
            ListIterator<TargetElement>
    {
        private final ListIterator<SourceElement> s;
        
        

        /**
         * @param s
         */
        public ListIteratorImplementation(ListIterator<SourceElement> s)
        {
            this.s = s;
        }

        @Override
        public boolean hasNext()
        {
            return s.hasNext();
        }

        @Override
        public TargetElement next()
        {
            return transformer.transform(s.next());
        }

        @Override
        public boolean hasPrevious()
        {
            return s.hasPrevious();
        }

        @Override
        public TargetElement previous()
        {
            return transformer.transform(s.previous());
        }

        @Override
        public int nextIndex()
        {
            return s.nextIndex();
        }

        @Override
        public int previousIndex()
        {
            return s.previousIndex();
        }

        @Override
        public void remove()
        {
            s.remove();
        }

        @Override
        public void set(TargetElement e)
        {
            throw new UnsupportedOperationException(
                    "Method TransformingList.listIterator().new ListIterator<TargetElement>() {...}.set(e) is not supported");
        }

        @Override
        public void add(TargetElement e)
        {
            throw new UnsupportedOperationException(
                    "Method TransformingList.listIterator().new ListIterator<TargetElement>() {...}.add(e) is not supported");
        }
    }
    
}
