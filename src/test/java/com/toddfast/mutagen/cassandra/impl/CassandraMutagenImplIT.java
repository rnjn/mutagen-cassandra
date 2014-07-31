package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.cassandra.CassandraMutagen;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.SchemaChangeResult;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.*;

/**
 *
 * @author Todd Fast
 */
public class CassandraMutagenImplIT {
	
	private static AstyanaxContext<Keyspace> context;
	
	private static Keyspace keyspace;

	public CassandraMutagenImplIT() {
	}

	@Before
	public void setUp() throws ConnectionException {
		defineKeyspace();
		createKeyspace();
	}


	@After
	public void tearDown() throws ConnectionException {
		OperationResult<SchemaChangeResult> result=keyspace.dropKeyspace();
		System.out.println("Dropped keyspace "+keyspace);
	}


	/**
	 * This is it!
	 *
	 */
	private Plan.Result<Integer> mutate()
			throws IOException {

		// Get an instance of CassandraMutagen
		// Using Nu: CassandraMutagen mutagen=$(CassandraMutagen.class);
		CassandraMutagen mutagen=new CassandraMutagenImpl();

		// Initialize the list of mutations
		String rootResourcePath="com/toddfast/mutagen/cassandra/test/mutations";
		mutagen.initialize(rootResourcePath);

		// Mutate!
		Plan.Result<Integer> result=mutagen.mutate(keyspace);

		return result;
	}

	@Test
	public void testInitialize() throws Exception {

		Plan.Result<Integer> result = mutate();

		// Check the results
		State<Integer> state=result.getLastState();

		System.out.println("Mutation complete: "+result.isMutationComplete());
		System.out.println("Exception: "+result.getException());
		if (result.getException()!=null) {
			result.getException().printStackTrace();
		}
		System.out.println("Completed mutations: "+result.getCompletedMutations());
		System.out.println("Remining mutations: "+result.getRemainingMutations());
		System.out.println("Last state: "+(state!=null ? state.getID() : "null"));

		assertTrue(result.isMutationComplete());
		assertNull(result.getException());
		assertEquals((state!=null ? state.getID() : (Integer)(-1)),(Integer)5);
	}


	/**
	 *
	 *
	 */
	@Test
	public void testData() throws Exception {
		
		mutate();
		
		final ColumnFamily<String,String> CF_TEST1=
			ColumnFamily.newColumnFamily("Test1",
				StringSerializer.get(),StringSerializer.get());

		ColumnList<String> columns;
		columns=keyspace.prepareQuery(CF_TEST1)
			.getKey("row1")
			.execute()
			.getResult();

		assertEquals("foo1",columns.getStringValue("value1",null));
		assertEquals("bar1",columns.getStringValue("value2",null));

		columns=keyspace.prepareQuery(CF_TEST1)
			.getKey("row2")
			.execute()
			.getResult();

		assertEquals("chickens",columns.getStringValue("value1",null));
		assertEquals("sneezes",columns.getStringValue("value2",null));

		columns=keyspace.prepareQuery(CF_TEST1)
			.getKey("row3")
			.execute()
			.getResult();

		assertEquals("bar",columns.getStringValue("value1",null));
		assertEquals("baz",columns.getStringValue("value2",null));
	}

	private static void defineKeyspace() {
		context=new AstyanaxContext.Builder()
			.forKeyspace("mutagen_test")
			.withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
				.setDefaultReadConsistencyLevel(ConsistencyLevel.CL_QUORUM)
				.setDefaultWriteConsistencyLevel(ConsistencyLevel.CL_QUORUM)
			)
			.withConnectionPoolConfiguration(
				new ConnectionPoolConfigurationImpl("testPool")
				.setPort(9160)
				.setMaxConnsPerHost(1)
				.setSeeds("localhost")
			)
			.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
			.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		keyspace=context.getClient();
	}
	
	private static void createKeyspace()
			throws ConnectionException {

		System.out.println("Creating keyspace "+keyspace+"...");

		int keyspaceReplicationFactor=1;

		Map<String, Object> keyspaceConfig=
			new HashMap<String, Object>();
		keyspaceConfig.put("strategy_options",
			ImmutableMap.<String, Object>builder()
				.put("replication_factor",
					""+keyspaceReplicationFactor)
				.build());

		String keyspaceStrategyClass="SimpleStrategy";
		keyspaceConfig.put("strategy_class",keyspaceStrategyClass);

		OperationResult<SchemaChangeResult> result=
			keyspace.createKeyspace(
				Collections.unmodifiableMap(keyspaceConfig));

		System.out.println("Created keyspace "+keyspace);
	}

}
