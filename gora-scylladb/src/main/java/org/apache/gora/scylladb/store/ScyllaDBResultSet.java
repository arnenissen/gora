package org.apache.gora.scylladb.store;

import org.apache.gora.cassandra.query.CassandraResultSet;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.query.Query;
import org.apache.gora.store.DataStore;

public class ScyllaDBResultSet<K, T extends Persistent> extends CassandraResultSet<K, T >  {
    /**
     * Constructor of the ScyllaDB Result
     *
     * @param dataStore Cassandra Data Store
     * @param query     Cassandra Query
     */
    public ScyllaDBResultSet(DataStore<K, T> dataStore, Query<K, T> query) {
        super(dataStore, query);
    }
}
