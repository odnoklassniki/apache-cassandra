/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package org.apache.cassandra.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

/**
 * The ConsistencyLevel is an enum that controls both read and write behavior based on <ReplicationFactor> in your
 * storage-conf.xml. The different consistency levels have different meanings, depending on if you're doing a write or read
 * operation. Note that if W + R > ReplicationFactor, where W is the number of nodes to block for on write, and R
 * the number to block for on reads, you will have strongly consistent behavior; that is, readers will always see the most
 * recent write. Of these, the most interesting is to do QUORUM reads and writes, which gives you consistency while still
 * allowing availability in the face of node failures up to half of <ReplicationFactor>. Of course if latency is more
 * important than consistency then you can use lower values for either or both.
 * 
 * Write:
 *      ZERO    Ensure nothing. A write happens asynchronously in background
 *      ANY     Ensure that the write has been written once somewhere, including possibly being hinted in a non-target node.
 *      ONE     Ensure that the write has been written to at least 1 node's commit log and memory table before responding to the client.
 *      QUORUM  Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes before responding to the client.
 *      ALL     Ensure that the write is written to <code>&lt;ReplicationFactor&gt;</code> nodes before responding to the client.
 * 
 * Read:
 *      ZERO    Not supported, because it doesn't make sense.
 *      ANY     Not supported. You probably want ONE instead.
 *      ONE     Will return the record returned by the first node to respond. A consistency check is always done in a
 *              background thread to fix any consistency issues when ConsistencyLevel.ONE is used. This means subsequent
 *              calls will have correct data even if the initial read gets an older value. (This is called 'read repair'.)
 *      QUORUM  Will query all storage nodes and return the record with the most recent timestamp once it has at least a
 *              majority of replicas reported. Again, the remaining replicas will be checked in the background.
 *      ALL     Queries all storage nodes and returns the record with the most recent timestamp.
 */
public enum ConsistencyLevel implements org.apache.thrift.TEnum {
  ZERO(0),
  ONE(1),
  QUORUM(2),
  DCQUORUM(3),
  DCQUORUMSYNC(4),
  ALL(5),
  ANY(6);

  private final int value;

  private ConsistencyLevel(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static ConsistencyLevel findByValue(int value) { 
    switch (value) {
      case 0:
        return ZERO;
      case 1:
        return ONE;
      case 2:
        return QUORUM;
      case 3:
        return DCQUORUM;
      case 4:
        return DCQUORUMSYNC;
      case 5:
        return ALL;
      case 6:
        return ANY;
      default:
        return null;
    }
  }
}
