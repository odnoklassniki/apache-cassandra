/*
 * @(#) Types.java
 * Created 21 сент. 2016 г. by oleg
 * (C) Odnoklassniki.ru
 */
package org.apache.cassandra.db.marshal;

import java.util.HashMap;

/**
 * @author Oleg Anastasyev<oa@odnoklassniki.ru>
 *
 */
public class Types
{
    // a copy on write cache used to avoid creating Type instances on deserializaion of CF
    private static volatile HashMap<String, AbstractType> types = new HashMap<String, AbstractType>();


    public static synchronized AbstractType register(AbstractType type) {
        AbstractType t = types.get( type );
        if ( t != null )
            return t;
        
        HashMap<String, AbstractType> newtypes = new HashMap<String, AbstractType>( types );
        newtypes.put( type.canonicalName, type );
        types = newtypes;
        
        return type;
    }
    
    public static AbstractType get( String canonicalName ) {
        AbstractType type = types.get( canonicalName );
        if ( type != null ) {
            return type;
        }
        
        try
        {
            return (AbstractType)Class.forName(canonicalName).getConstructor().newInstance();
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Unable to load comparator class '" + canonicalName + "'.  probably this means you have obsolete sstables lying around", e);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }
    
}
