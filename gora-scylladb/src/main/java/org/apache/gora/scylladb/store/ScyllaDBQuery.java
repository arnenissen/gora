package org.apache.gora.scylladb.store;

import org.apache.gora.cassandra.query.CassandraQuery;
import org.apache.gora.persistency.Persistent;
import org.apache.gora.store.DataStore;

public class ScyllaDBQuery<K, T extends Persistent> extends CassandraQuery<K, T > {

    public ScyllaDBQuery(DataStore<K, T> dataStore) {
        super(dataStore);
    }
}
