package com.facebook.LinkBench.store.mysql;

import com.facebook.LinkBench.store.LinkStoreFactory;
import com.facebook.LinkBench.store.LinkStore;
import com.facebook.LinkBench.store.NodeStore;

/**
 * {@link LinkStoreFactory} for MySQL.
 */
public class MysqlLinkStoreFactory implements LinkStoreFactory {

    @Override
    public LinkStore createLinkStore() {
        return new LinkStoreMysql();
    }
}
