package com.facebook.LinkBench.store;

/**
 * Implementations create instances of {@link LinkStore}.
 */
public interface LinkStoreFactory {

    /**
     * Get an instance of {@link LinkStore}. This can be a new instance (for cases where the DB runs externally
     * and the LinkStore just connects to it, such as in the case of MySQL), or always the same instance
     * (in case the DB is embedded, i.e. in the JVM, like Neo4j).
     *
     * @return link store.
     */
    LinkStore createLinkStore();
}
