package com.facebook.LinkBench.neo4j;

import java.util.Properties;

import static org.neo4j.graphdb.DynamicRelationshipType.*;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import com.facebook.LinkBench.ConfigUtil;
import com.facebook.LinkBench.GraphStore;
import com.facebook.LinkBench.Link;
import com.facebook.LinkBench.Node;
import com.facebook.LinkBench.Phase;

public class GraphStoreNeo4jEmbedded extends GraphStore
{
    /**
     * Node Fields
     */
    private static final String NODE_ID = "_n_id";
    private static final String NODE_TYPE = "_n_type";
    private static final String NODE_VERSION = "_n_version";
    private static final String NODE_TIME = "_n_time";
    private static final String NODE_DATA = "_n_data";
    /**
     * Link Fields
     */
    private static final String LINK_ID1 = "_l_id1";
    private static final String LINK_ID2 = "_l_id2";
    private static final String LINK_TYPE = "_l_type";
    private static final String LINK_VISIBILITY = "_l_visibility";
    private static final String LINK_DATA = "_l_data";
    private static final String LINK_VERSION = "_l_version";
    private static final String LINK_TIME = "_l_time";
    /**
     * Config Fields
     */
    private static final String PATH = "path";
    private static final String BULK_INSERT = "neo4j_bulk_insert_batch";
    /**
     * Index Names
     */
    private static final String NODE_TYPE_INDEX = "_index_node_type";
    private static final String LINK_EXISTENCE_INDEX = "_index_link_existence";
    private static final String NODE_TYPE_INDEX_KEY = "_index_key_node_type";
    private static final String LINK_EXISTENCE_INDEX_KEY = "_index_key_link_existence";

    public static final int BULK_INSERT_DEFAULT = 1024;

    private final Logger logger = Logger.getLogger( ConfigUtil.LINKBENCH_LOGGER );

    private String path;
    // private ExecutionEngine queryEngine;
    private GraphDatabaseService db;
    private Index<org.neo4j.graphdb.Node> nodeTypeIndex;
    private Index<org.neo4j.graphdb.Relationship> linkExistenceIndex;

    private Phase phase;
    private int bulkInsertSize;

    /**
     * Initialization & Maintenance
     */

    public GraphStoreNeo4jEmbedded( Properties props )
    {
        super();
        // TODO do these defaults make sense?
        initialize( props, Phase.LOAD, 0 );
    }

    @Override
    public void initialize( Properties props, Phase currentPhase, int threadId )
    {
        this.path = ConfigUtil.getPropertyRequired( props, PATH );
        this.phase = currentPhase;
        this.bulkInsertSize = Integer.parseInt( props.getProperty( BULK_INSERT, Integer.toString( BULK_INSERT_DEFAULT ) ) );

        openConnection();
    }

    // Invoked on error, to clean, reset, reopened, etc. the connection
    @Override
    public void clearErrors( int threadID )
    {
        // TODO not tested, probably not right
        logger.info( "Reopening Neo4j connection in threadID " + threadID );
        closeConnection();
        openConnection();
    }

    @Override
    public void close()
    {
        closeConnection();
    }

    @Override
    public void resetNodeStore( String dbid, long startID ) throws Exception
    {
        // TODO Delete Nodes
        // TODO Delete Relationships
        // TODO Delete Indexes
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }

    private void openConnection()
    {
        this.db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( this.path ).newGraphDatabase();
        this.nodeTypeIndex = this.db.index().forNodes( NODE_TYPE_INDEX );
        this.linkExistenceIndex = this.db.index().forRelationships( LINK_EXISTENCE_INDEX );
        registerShutdownHook( this.db );
    }

    private void closeConnection()
    {
        this.db.shutdown();
    }

    /**
     * Operations
     */

    @Override
    public long addNode( String dbid, Node node ) throws Exception
    {
        // TODO use dbid?
        long id;
        org.neo4j.graphdb.Node neo4jNode = null;
        Transaction tx = this.db.beginTx();
        try
        {
            neo4jNode = this.db.createNode();
            id = neo4jNode.getId();
            neo4jNode.setProperty( NODE_ID, id );
            neo4jNode.setProperty( NODE_TYPE, node.type );
            neo4jNode.setProperty( NODE_VERSION, node.version );
            neo4jNode.setProperty( NODE_TIME, node.time );
            neo4jNode.setProperty( NODE_DATA, node.data );

            // TODO change index keys
            // TODO perhaps use one key for mixed TYPE and ID
            this.nodeTypeIndex.add( neo4jNode, NODE_TYPE_INDEX_KEY, node.type );
            tx.success();
        }
        catch ( Exception e )
        {
            logger.error( "Error adding Node", e.getCause() );
            tx.failure();
            return -1;
        }
        finally
        {
            tx.finish();
        }
        return id;
    }

    @Override
    public Node getNode( String dbid, int type, long id ) throws Exception
    {
        // TODO use dbid?
        Node node;
        Transaction tx = this.db.beginTx();
        try
        {
            // String luceneQueryString = String.format( "_type:%s AND _id:%s",
            // type, id );
            // IndexHits<org.neo4j.graphdb.Node> hits = this.typeIndex.query(
            // luceneQueryString );
            // org.neo4j.graphdb.Node neo4jNode = hits.getSingle();

            // TODO does this work? is this fastest?
            org.neo4j.graphdb.Node neo4jNode = this.db.getNodeById( id );
            node = neo4jNodeToNode( neo4jNode );
            tx.success();
        }
        catch ( Exception e )
        {
            logger.error( "Error getting Node", e.getCause() );
            tx.failure();
            return null;
        }
        finally
        {
            tx.finish();
        }
        return node;
    }

    @Override
    public boolean updateNode( String dbid, Node node ) throws Exception
    {
        // TODO use dbid?
        Transaction tx = this.db.beginTx();
        try
        {
            // TODO can ID or TYPE get updated here?
            // TODO should indexes get updated here?
            // TODO does this work? is this fastest?
            org.neo4j.graphdb.Node neo4jNode = this.db.getNodeById( node.id );
            neo4jNode.setProperty( NODE_ID, node.id );
            neo4jNode.setProperty( NODE_TYPE, node.type );
            neo4jNode.setProperty( NODE_VERSION, node.version );
            neo4jNode.setProperty( NODE_TIME, node.time );
            neo4jNode.setProperty( NODE_DATA, node.data );
            tx.success();
        }
        catch ( Exception e )
        {
            logger.error( "Error updating Node", e.getCause() );
            tx.failure();
            return false;
        }
        finally
        {
            tx.finish();
        }
        return true;
    }

    @Override
    public boolean deleteNode( String dbid, int type, long id ) throws Exception
    {
        // TODO use dbid?
        Transaction tx = this.db.beginTx();
        try
        {
            // TODO should indexes get updated here?
            // TODO does this work? is this fastest?
            org.neo4j.graphdb.Node neo4jNode = this.db.getNodeById( id );
            // TODO delete Relationships first
            neo4jNode.delete();
            tx.success();
        }
        catch ( Exception e )
        {
            logger.error( "Error deleting Node", e.getCause() );
            tx.failure();
            return false;
        }
        finally
        {
            tx.finish();
        }
        return true;
    }

    @Override
    public boolean addLink( String dbId, Link link, boolean noInverse ) throws Exception
    {
        // TODO how to use dbId?
        // TODO how to use noInverse?
        Transaction tx = this.db.beginTx();
        try
        {
            org.neo4j.graphdb.Relationship neo4jRelationship = getOrCreateLink( link.id1, link.id2, link.link_type );
            neo4jRelationship.setProperty( LINK_VISIBILITY, link.visibility );
            neo4jRelationship.setProperty( LINK_DATA, link.data );
            neo4jRelationship.setProperty( LINK_VERSION, link.version );
            neo4jRelationship.setProperty( LINK_TIME, link.time );
            this.linkExistenceIndex.add( neo4jRelationship, LINK_EXISTENCE_INDEX_KEY,
                    toComplexLinkId( link.id1, link.id2, link.link_type ) );
            tx.success();
        }
        catch ( Exception e )
        {
            logger.error( "Error adding Link", e.getCause() );
            tx.failure();
            return false;
        }
        finally
        {
            tx.finish();
        }
        return true;
    }

    @Override
    public boolean deleteLink( String dbId, long id1, long linkType, long id2, boolean noInverse, boolean expunge )
            throws Exception
    {
        // TODO how to use dbId?
        // TODO how to use noInverse?
        // TODO remove entries from index?
        Transaction tx = this.db.beginTx();
        try
        {
            org.neo4j.graphdb.Relationship neo4jRelationship = getLinkIfExists( id1, id2, linkType );
            neo4jRelationship.delete();
            tx.success();
        }
        catch ( Exception e )
        {
            logger.error( "Error deleting Link", e.getCause() );
            tx.failure();
            return false;
        }
        finally
        {
            tx.finish();
        }
        return true;
    }

    @Override
    public boolean updateLink( String dbid, Link a, boolean noinverse ) throws Exception
    {
        // TODO
        return false;
    }

    @Override
    public Link getLink( String dbid, long id1, long link_type, long id2 ) throws Exception
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Link[] getLinkList( String dbid, long id1, long link_type ) throws Exception
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Link[] getLinkList( String dbid, long id1, long link_type, long minTimestamp, long maxTimestamp, int offset,
            int limit ) throws Exception
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long countLinks( String dbid, long id1, long link_type ) throws Exception
    {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Helpers
     */

    private Node neo4jNodeToNode( org.neo4j.graphdb.Node neo4jNode )
    {
        long id = (long) neo4jNode.getProperty( NODE_ID );
        int type = (int) neo4jNode.getProperty( NODE_TYPE );
        long version = (long) neo4jNode.getProperty( NODE_VERSION );
        int time = (int) neo4jNode.getProperty( NODE_TIME );
        byte data[] = (byte[]) neo4jNode.getProperty( NODE_DATA );
        Node node = new Node( id, type, version, time, data );
        return node;
    }

    private String toComplexLinkId( long startId, long endId, long linkType )
    {
        // TODO should take linkType too
        return String.format( "%s-%s-%s", startId, endId, linkType );
    }

    // Call from within transaction
    private org.neo4j.graphdb.Relationship getOrCreateLink( long startId, long endId, long linkType )
    {
        try
        {
            org.neo4j.graphdb.Relationship neo4jRelationship = getLinkIfExists( startId, endId, linkType );
            return ( null == neo4jRelationship ) ? createLink( startId, endId, linkType ) : neo4jRelationship;
        }
        catch ( Exception e )
        {
            logger.error( "Error getting/adding Link", e.getCause() );
            return null;
        }
    }

    // Call from within transaction
    private org.neo4j.graphdb.Relationship createLink( long startId, long endId, long linkType )
    {
        try
        {
            org.neo4j.graphdb.Node neo4jNodeStart = this.db.getNodeById( startId );
            org.neo4j.graphdb.Node neo4jNodeEnd = this.db.getNodeById( endId );
            return neo4jNodeStart.createRelationshipTo( neo4jNodeEnd, withName( Long.toString( linkType ) ) );
        }
        catch ( Exception e )
        {
            logger.error( "Error adding Link", e.getCause() );
            return null;
        }
    }

    // Call from within transaction
    private org.neo4j.graphdb.Relationship getLinkIfExists( long startId, long endId, long linkType )
    {
        try
        {
            // TODO should this check in both directions?
            IndexHits<org.neo4j.graphdb.Relationship> hits = this.linkExistenceIndex.get( LINK_EXISTENCE_INDEX_KEY,
                    toComplexLinkId( startId, endId, linkType ) );
            return hits.getSingle();
        }
        catch ( Exception e )
        {
            logger.error( "Error gettingLink if exists", e.getCause() );
            return null;
        }
    }
}
