package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;

import static com.toddfast.mutagen.cassandra.VersionTable.VERSION_TABLE;
import static com.toddfast.mutagen.cassandra.VersionTable.VERSION_TABLE_KEY;
import static com.toddfast.mutagen.cassandra.VersionTable.VERSION_TABLE_VALUE;
import static com.toddfast.mutagen.cassandra.VersionTable.createTableVersionTable;

/**
 *
 * @author Todd Fast
 */
public class CassandraSubject implements Subject<Integer> {
	
	private String keyspace;

	private Session session;

	public CassandraSubject(Session session, String keyspace) {
		super();
		if (keyspace==null) {
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
				.all()
				.from(VERSION_TABLE)
				.where(QueryBuilder.eq(VERSION_TABLE_KEY, getKeyspace()));
		
		Row tableState = session.execute(select).one();

		if(tableState != null) {
			currentVersion = tableState.getInt(VERSION_TABLE_VALUE);
		}
		return new SimpleState<Integer>(currentVersion);
	}
}
