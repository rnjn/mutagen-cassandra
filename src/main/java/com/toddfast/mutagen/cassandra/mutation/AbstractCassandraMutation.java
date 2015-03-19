package com.toddfast.mutagen.cassandra.mutation;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.CassandraSubject;

import java.nio.ByteBuffer;

import static com.toddfast.mutagen.cassandra.VersionTable.*;
import static com.toddfast.mutagen.cassandra.mutation.HashUtils.md5String;

/**
 * @author Todd Fast
 */
public abstract class AbstractCassandraMutation implements Mutation<Integer> {

    private CassandraSubject subject;

    protected AbstractCassandraMutation(CassandraSubject subject) {
        super();
        this.subject = subject;
    }

    protected CassandraSubject getSubject() {
        return subject;
    }

    @Override
    public abstract State<Integer> getResultingState();

    /**
     * Return a canonical representative of the change in string form
     */
    protected abstract String getChangeSummary();

    @Override
    public String toString() {
        if (getResultingState() != null) {
            return super.toString() + "[state=" + getResultingState().getID()
                    + "]";
        } else {
            return super.toString();
        }
    }

    /**
     * @param resourceName
     * @return
     */
    protected final State<Integer> parseVersion(String resourceName) {
        return new SimpleState<Integer>(MutationParser.parseMutationVersion(resourceName));
    }

    /**
     * Perform the actual mutation
     */
    protected abstract void performMutation(Context context);

    /**
     * Performs the actual mutation and then updates the recorded schema version
     */
    @Override
    public final void mutate(Context context) throws MutagenException {

        // Perform the mutation
        performMutation(context);

        Integer version = getResultingState().getID();
        String changeSummary = getChangeSummary();
        String changeHash = md5String(changeSummary);

        String keySpace = getSubject().getKeyspace();

        Insert insertVersionChange = QueryBuilder.insertInto(VERSION_TABLE)
                .value(VERSION_TABLE_KEY, version.toString())
                .value(VERSION_TABLE_ROW_TYPE, "change")
                .value(VERSION_TABLE_VALUE, changeSummary);

        Insert insertVersionHash = QueryBuilder.insertInto(VERSION_TABLE)
                .value(VERSION_TABLE_KEY, version.toString())
                .value(VERSION_TABLE_ROW_TYPE, "hash")
                .value(VERSION_TABLE_VALUE, changeHash);


        Update.Where updateState = QueryBuilder.update(VERSION_TABLE)
                .with(QueryBuilder.set(VERSION_TABLE_VALUE, ByteBuffer.allocate(4).putInt(0, version)))
                .where(QueryBuilder.eq(VERSION_TABLE_KEY, "state"))
                .and(QueryBuilder.eq(VERSION_TABLE_ROW_TYPE, "version"));



        try {
            getSubject().getSession().execute(insertVersionChange);
            getSubject().getSession().execute(insertVersionHash);
            getSubject().getSession().execute(updateState);

        } catch (Exception e) {
            throw new MutagenException("Could not update " + VERSION_TABLE
                    + " column family to state " + version + " for keySpace: " + keySpace
                    + "; schema is now out of sync with recorded version", e);
        }
    }

}
