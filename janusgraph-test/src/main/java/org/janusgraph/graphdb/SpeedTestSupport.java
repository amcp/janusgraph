package org.janusgraph.graphdb;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.query.QueryUtil;
import org.apache.tinkerpop.gremlin.structure.io.graphml.GraphMLReader;
import org.apache.tinkerpop.gremlin.util.Gremlin;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.diskstorage.BackendException;

import com.google.common.collect.Iterables;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.zip.GZIPInputStream;

abstract class SpeedTestSupport {

    private static final Logger log = LoggerFactory.getLogger(SpeedTestSupport.class);

    @Rule
    public TestName testName = new TestName();

    // Graph generation settings
    public static final int VERTEX_COUNT = SpeedTestSchema.VERTEX_COUNT;
    public static final int EDGE_COUNT = SpeedTestSchema.EDGE_COUNT;

    // Query execution setting defaults
    public static final int DEFAULT_TX_COUNT = 3;
    public static final int DEFAULT_VERTICES_PER_TX = 100;
    public static final int DEFAULT_ITERATIONS = DEFAULT_TX_COUNT * DEFAULT_VERTICES_PER_TX;

    public static final String RELATION_FILE = "../janusgraph-test/data/v10k.graphml.gz";

    // Mutable state

    /*  JUnit constructs a new test class instance before executing each test method. 
     * Ergo, each test method gets its own Random instance. 
     * The seed is arbitrary and carries no special significance,
     * but we keep the see fixed for repeatability.
     */
    protected Random random = new Random(7);
    protected SpeedTestSchema schema;
    protected JanusGraph graph;
    protected WriteConfiguration conf;

    protected SpeedTestSupport(WriteConfiguration conf) throws BackendException {
        this.conf = conf;
    }

    @Before
    void open() {
//        Preconditions.checkArgument(TX_COUNT * DEFAULT_OPS_PER_TX <= VERTEX_COUNT);

        if (null == graph) {
            try {
                graph = getGraph();
            } catch (BackendException e) {
                throw new RuntimeException(e);
            }
        }
        if (null == schema) {
            schema = getSchema();
        }
    }

    @After
    void rollback() {
        if (null != graph)
            graph.tx().rollback();
    }

    void close() {
        if (null != graph)
            graph.close();
    }

    protected abstract StandardJanusGraph getGraph() throws BackendException;

    protected abstract SpeedTestSchema getSchema();

    /*
     * Helper methods
     */

    protected void sequentialUidTask(Integer verticesPerTx, BiConsumer<JanusGraphTransaction, JanusGraphVertex> consumer) {
        this.chunkedSequentialUidTask(1,
        		verticesPerTx == null ? DEFAULT_VERTICES_PER_TX : verticesPerTx,
        		(tx, vbuf, vloaded) -> {
		            assert 1 == vloaded;
		            assert 1 == vbuf.length;
		            JanusGraphVertex v = vbuf[0];
		            consumer.accept(tx, v);
        });
    }

    protected void chunkedSequentialUidTask(Integer chunksize,
    		Integer verticesPerTx, TriConsumer<JanusGraphTransaction, JanusGraphVertex[], Integer> consumer) {
    	int actualChunksize = chunksize == null ? DEFAULT_VERTICES_PER_TX : chunksize;
    	int actualVerts = verticesPerTx == null ? DEFAULT_VERTICES_PER_TX : verticesPerTx;
        /*
         * Need this condition because of how we handle transactions and buffer
         * Vertex objects.  If this divisibility constraint were violated, then
         * we would end up passing Vertex instances from one or more committed
         * transactions as if those instances were not stale.
         */
        Preconditions.checkArgument(0 == actualVerts % chunksize);

        long count = DEFAULT_TX_COUNT * actualVerts;
        long offset = Math.abs(random.nextLong()) % schema.getMaxUid();
        SequentialLongIterator uids = new SequentialLongIterator(count, schema.getMaxUid(), offset);
        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex[] vbuf = new JanusGraphVertex[chunksize];
        int vloaded = 0;

        while (uids.hasNext()) {
            long u = uids.next();
            JanusGraphVertex v = Iterables.getOnlyElement(QueryUtil.getVertices(tx, Schema.UID_PROP, u));
            assertNotNull(v);
            vbuf[vloaded++] = v;
            if (vloaded == chunksize) {
                consumer.accept(tx, vbuf, vloaded);
                vloaded = 0;
                tx.commit();
                tx = graph.newTransaction();
            }
        }

        if (0 < vloaded) {
            consumer.accept(tx, vbuf, vloaded);
            tx.commit();
        } else {
            tx.rollback();
        }
    }

    protected void supernodeTask(TriConsumer<JanusGraphVertex, String, String> closure) {
        long uid = schema.getSupernodeUid();
        String label = schema.getSupernodeOutLabel();
        assertNotNull(label);
        String pkey = schema.getSortKeyForLabel(label);
        assertNotNull(pkey);

        JanusGraphTransaction tx = graph.newTransaction();
        JanusGraphVertex v = Iterables.getOnlyElement(QueryUtil.getVertices(tx, Schema.UID_PROP, uid));
//            def v = graph.V(Schema.UID_PROP, uid).next()
        assertNotNull(v);
        closure.accept(v, label, pkey);
        tx.commit();
    }

    protected void standardIndexEdgeTask(TriConsumer<JanusGraphTransaction, String, Integer> closure) {
        final int keyCount = schema.getEdgePropKeys();

        JanusGraphTransaction tx = graph.newTransaction();
        int value = -1;
        for (int p = 0; p < schema.getEdgePropKeys(); p++) {
            for (int i = 0; i < 5; i++) {
                if (++value >= schema.getMaxEdgePropVal())
                    value = 0;
                closure.accept(tx, schema.getEdgePropertyName(p), value);
            }
        }
        tx.commit();
    }

    protected void standardIndexVertexTask(TriConsumer<JanusGraphTransaction, String, Integer> closure) {
        final int keyCount = schema.getVertexPropKeys();

        JanusGraphTransaction tx = graph.newTransaction();
        int value = -1;
        for (int p = 0; p < schema.getVertexPropKeys(); p++) {
            for (int i = 0; i < 5; i++) {
                if (++value >= schema.getMaxVertexPropVal())
                    value = 0;
                closure.accept(tx, schema.getVertexPropertyName(p), value);
            }

        }
        tx.commit();
    }

    protected void initializeGraph(JanusGraph g) throws BackendException {
        log.info("Initializing graph...");
        long before = System.currentTimeMillis();
        SpeedTestSchema schema = getSchema();

        try {
            InputStream data = new GZIPInputStream(new FileInputStream(RELATION_FILE));
            schema.makeTypes(g);
            GraphMLReader.inputGraph(g, data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        long after = System.currentTimeMillis();
        long duration = after - before;
        if (15 * 1000 <= duration) {
            log.warn("Initialized graph (" + duration + " ms).");
        } else {
            log.info("Initialized graph (" + duration + " ms).");
        }
    }
}
