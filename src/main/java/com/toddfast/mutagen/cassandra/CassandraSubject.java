package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.SimpleState;

/**
 *
 * @author Todd Fast
 */
public class CassandraSubject implements Subject<Integer> {
	
	private Session session;
	
	public static final String ROW_KEY="state";
		
	public static final String VERSION_COLUMN="version";
	
	private static final String VERSION_CF = "create table schema_version ( version int primary key, change text, hash text  );";
	
	private static final String CURRENT_VERSION_CF = "create table schema_current_version ( state text primary key, version int ); ";


	public CassandraSubject(Session session) {
		super();
		if (session==null) {
			throw new IllegalArgumentException(
				"Parameter \"session\" cannot be null");
		}

		this.session=session;
	}

	public Session getSession() {
		return session;
	}

	private void createSchemaVersionTable() {
		try {
			session.execute(VERSION_CF);
		} catch (AlreadyExistsException e) {
			
		}
		try {
			session.execute(CURRENT_VERSION_CF);
		} catch (AlreadyExistsException e) {
			
		}
	}

	@Override
	public State<Integer> getCurrentState() {
		int current_version = 0;
		
		createSchemaVersionTable();

		String cql_version = "SELECT * FROM schema_current_version WHERE state = ?";

		BoundStatement statement = session.prepare(cql_version).bind(ROW_KEY);
		Row versionRow = session.execute(statement).one();
		
		if(versionRow != null) {
			current_version = versionRow.getInt(VERSION_COLUMN);
		}


		return new SimpleState<Integer>(current_version);
	}
}
