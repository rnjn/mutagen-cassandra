package com.toddfast.mutagen.cassandra;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

public class VersionTable {
    public final static String VERSION_TABLE = "schema_version";
    public final static String VERSION_TABLE_KEY = "key";
    public final static String VERSION_TABLE_VALUE = "value";
    public final static String VERSION_TABLE_ROW_TYPE = "column1";


    private static final String TABLE_VERSION_CF =
            "CREATE TABLE " + VERSION_TABLE + " ("
                    + VERSION_TABLE_KEY + " text,"
                    + VERSION_TABLE_ROW_TYPE + " text,"
                    + VERSION_TABLE_VALUE + " blob,"
                    + "PRIMARY KEY (" + VERSION_TABLE_KEY + "," + VERSION_TABLE_ROW_TYPE +"));";

    public static void createTableVersionTable(Session session) {
        // Make sure the versioning table exists.
        try {
            session.execute(TABLE_VERSION_CF);
        } catch (AlreadyExistsException e) {

        }
    }

}
