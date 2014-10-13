package org.apache.cassandra.maint;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * @author Dmitry Spikhalskiy <dmitry.spikhalskiy@corp.mail.ru>
 */
public class ClusterSnapshotTaskTest {
    @Test
    public void testTrivialConfigParsing() {
        ClusterSnapshotTask task = new ClusterSnapshotTask("night");
        assertEquals("night", task.tagname);
        assertNull(task.cfRegExp);
    }

    @Test
    public void testRegExpConfigParsing() {
        ClusterSnapshotTask task = new ClusterSnapshotTask("night(data1(_\\w{2})?)");
        assertEquals("night", task.tagname);
        assertEquals("data1(_\\w{2})?", task.cfRegExp);
    }
}
