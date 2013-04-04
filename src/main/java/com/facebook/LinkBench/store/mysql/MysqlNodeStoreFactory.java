package com.facebook.LinkBench.store.mysql;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.NodeStore;
import com.facebook.LinkBench.store.NodeStoreFactory;

/**
 * {@link NodeStoreFactory} for MySQL.
 */
public class MysqlNodeStoreFactory implements NodeStoreFactory {

    @Override
    public NodeStore createNodeStore(LinkStore linkStore) {
        if (!(linkStore instanceof NodeStore)) {
            throw new IllegalArgumentException("Provided LinkStore is not a NodeStore as well. This is a requirement" +
                    "for MySQL implementation.");
        }

        return (NodeStore) linkStore;
    }
}
