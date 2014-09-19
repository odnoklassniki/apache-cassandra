package org.apache.cassandra.service;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.utils.Pair;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Read operation timeout exception based on RpcTimeoutInMillis property in storage configuration
 *
 * Created by sergey.trevgoda on 15.09.14.
 */
public class ReadTimeoutException extends TimeoutException {

    private final List<InetAddress> successReadNodes;
    private final List<InetAddress> failReadNodes;
    private final String detailedMessage;

    private ReadTimeoutException(String message, String detailedMessage, List<InetAddress> successReadNodes, List<InetAddress> failReadNodes) {
        super(message);
        this.detailedMessage = detailedMessage;
        this.successReadNodes = successReadNodes;
        this.failReadNodes = failReadNodes;
    }

    /**
     * Quorum response timeout case (maybe some nodes are slow)
     * @param endPointList - the list of replicas nodes based on key
     * @param responses - the list of received responses
     *                  (initially fill with nulls, endPointList.size length - then populated with values from nodes)
     * @return - exception with detailed info
     */
    public static ReadTimeoutException quorumTimeout(List<InetAddress> endPointList, AtomicReferenceArray<Pair<InetAddress, ReadResponse>> responses) {
        int endPointListSize = endPointList.size();
        List<InetAddress> successReadNodes = new ArrayList<InetAddress>(endPointListSize);
        List<InetAddress> failReadNodes = new ArrayList<InetAddress>(endPointListSize);
        for (int i = 0; i < responses.length(); i++) {
            Pair<InetAddress, ReadResponse> pair = responses.get(i);
            if (pair != null) { // we have response within given timeout
                successReadNodes.add(pair.left);
            }
        }
        for (InetAddress endPoint : endPointList) {
            if (!successReadNodes.contains(endPoint)) {
                failReadNodes.add(endPoint);
            }
        }
        String detailedMessage = String.format("Quorum read timed out: success nodes: [%s], failed nodes : [%s]",
                getNodesList(successReadNodes), getNodesList(failReadNodes));
        return new ReadTimeoutException("Parallel read operation timed out", detailedMessage, successReadNodes, failReadNodes);
    }

    public String getDetailedMessage() {
        return detailedMessage;
    }

    public List<InetAddress> getSuccessReadNodes() {
        return successReadNodes;
    }

    public List<InetAddress> getFailReadNodes() {
        return failReadNodes;
    }

    private static String getNodesList(List<InetAddress> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            return "";
        }
        StringBuilder str = new StringBuilder();
        for (InetAddress node : nodes) {
            str.append(node.getHostAddress()).append(", ");
        }
        int length = str.length();
        if (length > 0) {
            str.setLength(length - 2);
        }
        return str.toString();
    }
}
