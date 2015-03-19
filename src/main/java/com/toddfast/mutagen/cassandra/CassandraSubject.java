package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;

import java.nio.ByteBuffer;

import static com.toddfast.mutagen.cassandra.VersionTable.*;

/**
 * @author Todd Fast
 */
public class CassandraSubject implements Subject<Integer> {

    private String keyspace;

    private Session session;

    public CassandraSubject(Session session, String keyspace) {
        super();
        if (keyspace == null) {
            throw new IllegalArgumentException(
                    "Parameter \"Table\" cannot be null");
        }
        this.session = session;
        this.keyspace = keyspace;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public Session getSession() {
        return session;
    }

    @Override
    public State<Integer> getCurrentState() {
        int currentVersion = 0;

        createTableVersionTable(getSession());

        Select.Where select = QueryBuilder.select()
                .column(VERSION_TABLE_VALUE)
                .from(VERSION_TABLE)
                .where(QueryBuilder.eq(VERSION_TABLE_KEY, "state"))
                .and(QueryBuilder.eq(VERSION_TABLE_ROW_TYPE, "version"));

        Row state = session.execute(select).one();

        if (state != null) {
            ByteBuffer bytes = state.getBytes(VERSION_TABLE_VALUE);
            currentVersion = bytes.asIntBuffer().get();
        }
        return new SimpleState<Integer>(currentVersion);
    }
}
