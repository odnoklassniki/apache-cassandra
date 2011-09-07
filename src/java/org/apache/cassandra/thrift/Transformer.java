/*
 * @(#) Transformer.java
 * Created Jun 16, 2011 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.thrift;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public interface Transformer<Source,Target>
{
    Target transform(Source o);
}
