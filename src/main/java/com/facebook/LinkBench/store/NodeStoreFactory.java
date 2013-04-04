package com.facebook.LinkBench.store;

/**
 * Implementations create instances of {@link NodeStore}.
 */
public interface NodeStoreFactory {

    /**
     * Get an instance of {@link NodeStore}. This can be a new instance (for cases where the DB runs externally
     * and the NodeStore just connects to it, such as in the case of MySQL), or always the same instance
     * (in case the DB is embedded, i.e. in the JVM, like Neo4j).
     *
     * @param linkStore already created LinkStore to give the implementations a chance to use the same object for both
     *                  LinkStore and NodeStore.
     * @return node store.
     */
    NodeStore createNodeStore(LinkStore linkStore);
}
