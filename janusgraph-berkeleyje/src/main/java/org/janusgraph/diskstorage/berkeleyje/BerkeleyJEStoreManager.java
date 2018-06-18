// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.berkeleyje;


import com.google.common.base.Preconditions;
import com.sleepycat.je.*;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.common.LocalStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.MergedConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.util.system.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IN_MEMORY;

@PreInitializeConfigOptions
public class BerkeleyJEStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJEStoreManager.class);

    public static final ConfigNamespace BERKELEY_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "berkeleyje", "BerkeleyDB JE configuration options");

    public static final ConfigOption<Integer> JVM_CACHE =
            new ConfigOption<Integer>(BERKELEY_NS,"cache-percentage",
            "Percentage of JVM heap reserved for BerkeleyJE's cache",
            ConfigOption.Type.MASKABLE, 65, ConfigOption.positiveInt());

    public static final ConfigOption<String> LOCK_MODE =
            new ConfigOption<>(BERKELEY_NS, "lock-mode",
            "The BDB record lock mode used for read operations",
            ConfigOption.Type.MASKABLE, String.class, LockMode.DEFAULT.toString(), disallowEmpty(String.class));

    public static final ConfigOption<String> ISOLATION_LEVEL =
            new ConfigOption<>(BERKELEY_NS, "isolation-level",
            "The isolation level used by transactions",
            ConfigOption.Type.MASKABLE,  String.class,
            IsolationLevel.REPEATABLE_READ.toString(), disallowEmpty(String.class));

    private final Map<String, BerkeleyJEKeyValueStore> stores;

    protected Environment environment;
    protected final StoreFeatures features;

    public BerkeleyJEStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new HashMap<>();

        int cachePercentage = configuration.get(JVM_CACHE);
        initialize(cachePercentage);

        features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(transactional)
                    .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .scanTxConfig(GraphDatabaseConfiguration.buildGraphConfiguration()
                            .set(ISOLATION_LEVEL, IsolationLevel.READ_UNCOMMITTED.toString()))
                    .supportsInterruption(false)
                    .optimisticLocking(false)
                    .build();

//        features = new StoreFeatures();
//        features.supportsOrderedScan = true;
//        features.supportsUnorderedScan = false;
//        features.supportsBatchMutation = false;
//        features.supportsTxIsolation = transactional;
//        features.supportsConsistentKeyOperations = true;
//        features.supportsLocking = true;
//        features.isKeyOrdered = true;
//        features.isDistributed = false;
//        features.hasLocalKeyPartition = false;
//        features.supportsMultiQuery = false;
    }

    private void initialize(int cachePercent) throws BackendException {
        try {

            final EnvironmentConfig envConfig;

            if (storageConfig.get(IN_MEMORY)) {
                Properties properties = new Properties();
                properties.put(EnvironmentConfig.LOG_MEM_ONLY, "true");
                envConfig = new EnvironmentConfig(properties);
            } else {
                envConfig = new EnvironmentConfig();
            }

            envConfig.setAllowCreate(true);
            envConfig.setTransactional(transactional);
            envConfig.setCachePercent(cachePercent);

            if (batchLoading) {
                envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
                envConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
            }

            //Open the environment
            environment = new Environment(directory, envConfig);


        } catch (DatabaseException e) {
            throw new PermanentBackendException("Error during BerkeleyJE initialization: ", e);
        }

    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BerkeleyJETx beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        try {
            Transaction tx = null;

            Configuration effectiveCfg =
                    new MergedConfiguration(txCfg.getCustomOptions(), getStorageConfig());

            if (transactional) {
                TransactionConfig txnConfig = new TransactionConfig();
                ConfigOption.getEnumValue(effectiveCfg.get(ISOLATION_LEVEL),IsolationLevel.class).configure(txnConfig);
                tx = environment.beginTransaction(null, txnConfig);
            }
            BerkeleyJETx btx = new BerkeleyJETx(tx, ConfigOption.getEnumValue(effectiveCfg.get(LOCK_MODE),LockMode.class), txCfg);

            if (log.isTraceEnabled()) {
                log.trace("Berkeley tx created", new TransactionBegin(btx.toString()));
            }

            return btx;
        } catch (DatabaseException e) {
            throw new PermanentBackendException("Could not start BerkeleyJE transaction", e);
        }
    }

    @Override
    public BerkeleyJEKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            BerkeleyJEKeyValueStore store = stores.get(name);
            return store;
        }
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setReadOnly(false);
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(transactional);

            dbConfig.setKeyPrefixing(true);

            if (batchLoading) {
                dbConfig.setDeferredWrite(true);
            }

            Database db = environment.openDatabase(null, name, dbConfig);

            log.debug("Opened database {}", name, new Throwable());

            BerkeleyJEKeyValueStore store = new BerkeleyJEKeyValueStore(name, db, this);
            stores.put(name, store);
            return store;
        } catch (DatabaseException e) {
            throw new PermanentBackendException("Could not open BerkeleyJE data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String,KVMutation> muts : mutations.entrySet()) {
            BerkeleyJEKeyValueStore store = openDatabase(muts.getKey());
            KVMutation mut = muts.getValue();

            if (!mut.hasAdditions() && !mut.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", muts.getKey());
            } else {
                log.debug("Mutating {}", muts.getKey());
            }

            if (mut.hasAdditions()) {
                for (KeyValueEntry entry : mut.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    log.trace("Insertion on {}: {}", muts.getKey(), entry);
                }
            }
            if (mut.hasDeletions()) {
                for (StaticBuffer del : mut.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", muts.getKey(), del);
                }
            }
        }
    }

    void removeDatabase(BerkeleyJEKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }


    @Override
    public void close() throws BackendException {
        if (environment != null) {
            if (!stores.isEmpty())
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            try {
                // TODO this looks like a race condition
                //Wait just a little bit before closing so that independent transaction threads can clean up.
                Thread.sleep(30);
            } catch (InterruptedException e) {
                //Ignore
            }
            try {
                environment.close();
            } catch (DatabaseException e) {
                throw new PermanentBackendException("Could not close BerkeleyJE database", e);
            }
        }

    }

    @Override
    public void clearStorage() throws BackendException {
        if (!stores.isEmpty())
            throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet().toString());

        Transaction tx = null;
        for (String db : environment.getDatabaseNames()) {
            environment.removeDatabase(tx, db);
            log.debug("Removed database {} (clearStorage)", db);
        }
        close();
        IOUtils.deleteFromDirectory(directory);
    }

    @Override
    public boolean exists() throws BackendException {
        return !environment.getDatabaseNames().isEmpty();
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }


    public static enum IsolationLevel {
        READ_UNCOMMITTED {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setReadUncommitted(true);
            }
        }, READ_COMMITTED {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setReadCommitted(true);

            }
        }, REPEATABLE_READ {
            @Override
            void configure(TransactionConfig cfg) {
                // This is the default and has no setter
            }
        }, SERIALIZABLE {
            @Override
            void configure(TransactionConfig cfg) {
                cfg.setSerializableIsolation(true);
            }
        };

        abstract void configure(TransactionConfig cfg);
    };

    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }
}
