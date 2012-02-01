/*
 * @(#) ILocalCallback.java
 * Created Jan 31, 2012 by oleg
 * (C) ONE, SIA
 */
package org.apache.cassandra.service;

import org.apache.cassandra.db.Row;

/**
 * @author Oleg Anastasyev<oa@hq.one.lv>
 *
 */
public interface ILocalCallback
{

    void localResponse(Row data);

}