package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;

import com.yahoo.ycsb.ByteIterator;

public class Neo4jClientUtils
{
    // TODO test
    public static String toCypherPropertiesString( Map<String, ByteIterator> values, String nodeName )
    {
        StringBuilder sb = new StringBuilder();
        for ( String key : values.keySet() )
        {
            sb.append( nodeName );
            sb.append( "." );
            sb.append( key );
            sb.append( "={" );
            sb.append( key );
            sb.append( "}," );
        }
        return sb.toString().substring( 0, sb.toString().length() - 1 );

    }

    // TODO test
    public static Map<String, Object> toStringObjectMap( Map<String, ByteIterator> values )
    {
        Map<String, Object> cypherMap = new HashMap<String, Object>();
        for ( String key : values.keySet() )
        {
            cypherMap.put( key, values.get( key ).toString() );
        }
        return cypherMap;
    }

}
