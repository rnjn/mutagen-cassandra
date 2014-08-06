package com.toddfast.mutagen.cassandra.mutation;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.CassandraSubject;

import static com.toddfast.mutagen.cassandra.VersionTable.VERSION_TABLE;
import static com.toddfast.mutagen.cassandra.VersionTable.VERSION_TABLE_KEY;
import static com.toddfast.mutagen.cassandra.VersionTable.VERSION_TABLE_VALUE;

/**
 *
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
	 *
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
	 * 
	 * @param resourceName
	 * @return
	 */
	protected final State<Integer> parseVersion(String resourceName) {
		return new SimpleState<Integer>(MutationParser.parseMutationVersion(resourceName));
	}

	/**
	 * Perform the actual mutation
	 *
	 */
	protected abstract void performMutation(Context context);

	/**
	 * Performs the actual mutation and then updates the recorded schema version
	 *
	 */
	@Override
	public final void mutate(Context context) throws MutagenException {

		// Perform the mutation
		performMutation(context);

		int version = getResultingState().getID();
		
		String tableName = getSubject().getSubjectName();
		
		Update.Where updateVersion = QueryBuilder.update(VERSION_TABLE)
				.with(QueryBuilder.set(VERSION_TABLE_VALUE, version))
				.where(QueryBuilder.eq(VERSION_TABLE_KEY, tableName));
		
		try {
			getSubject().getSession().execute(updateVersion);
		} catch (Exception e) {
			throw new MutagenException("Could not update \"table_version\" "
					+ "column family to state " + version + "for table: " + tableName
					+ "; schema is now out of sync with recorded version", e);
		}
	}

}
