package com.facebook.LinkBench.store.memory;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.NodeStore;
import com.facebook.LinkBench.store.NodeStoreFactory;

/**
 * {@link com.facebook.LinkBench.store.NodeStoreFactory} for in-memory implementation.
 */
public class MemoryNodeStoreFactory implements NodeStoreFactory {

    @Override
    public NodeStore createNodeStore(LinkStore linkStore) {
        if (!(linkStore instanceof NodeStore)) {
            throw new IllegalArgumentException("Provided LinkStore is not a NodeStore as well. This is a requirement" +
                    "for Memory implementation.");
        }

        return (NodeStore) linkStore;
    }
}
