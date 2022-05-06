package com.wenbao.flink.mysql.source.iterator;

import java.util.Iterator;

/**
 * CoprocessorIterator
 *
 * @author wenbao
 * @since 2022/03/10
 */
public class CoprocessorIterator<T> implements Iterator<T> {
    
    private final Iterator<T> records;
    
    public CoprocessorIterator(Iterator<T> records) {
        this.records = records;
    }
    
    
    @Override
    public boolean hasNext() {
        return records.hasNext();
    }
    
    @Override
    public T next() {
        return records.next();
    }
    
    
}