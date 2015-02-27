package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

public class VersionTable {
	
	public final static String VERSION_TABLE = "keyspace_version";

	public final static String VERSION_TABLE_KEY = "keyspace_name";
	
	public final static String VERSION_TABLE_VALUE = "version";
	
	private static final String TABLE_VERSION_CF = 
			"CREATE TABLE " + VERSION_TABLE + " ("
			+ VERSION_TABLE_KEY + " varchar,"
			+ VERSION_TABLE_VALUE + " int,"
			+ "PRIMARY KEY (" + VERSION_TABLE_KEY+ "));";
	
	public static void createTableVersionTable(Session session) {
		// Make sure the versioning table exists.
		try {
			session.execute(TABLE_VERSION_CF);
		} catch (AlreadyExistsException e) {
			
		}
	}
	
}
