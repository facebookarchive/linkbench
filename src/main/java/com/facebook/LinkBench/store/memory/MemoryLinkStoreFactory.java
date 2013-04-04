package com.facebook.LinkBench.store.memory;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.LinkStoreFactory;

/**
 * {@link com.facebook.LinkBench.store.LinkStoreFactory} for in-memory implementation.
 */
public class MemoryLinkStoreFactory implements LinkStoreFactory {

    @Override
    public LinkStore createLinkStore() {
        return new MemoryLinkStore();
    }
}
