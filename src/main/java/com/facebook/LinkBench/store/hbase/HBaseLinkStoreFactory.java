package com.facebook.LinkBench.store.hbase;

import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.LinkStoreFactory;

/**
 * {@link LinkStoreFactory} for HBase.
 */
public class HBaseLinkStoreFactory implements LinkStoreFactory {

    @Override
    public LinkStore createLinkStore() {
        return new LinkStoreHBaseGeneralAtomicityTesting();
    }
}
