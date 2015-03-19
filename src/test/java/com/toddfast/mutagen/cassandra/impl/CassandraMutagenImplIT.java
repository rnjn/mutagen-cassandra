package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.State;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

/**
 *
 * @author Todd Fast
 */
public class CassandraMutagenImplIT {
	
	private static Session session;
	
	private static final String KEY_SPACE = "Keyspace1";
	
	public CassandraMutagenImplIT() {
	}

	@Before
	public void setUp() {
		defineSession();
		createKeyspace();
	}


	@After
	public void tearDown() {
		session.execute("DROP KEYSPACE " + KEY_SPACE + ";");
		session.close();
		System.out.println("Dropped keyspace "+KEY_SPACE);
	}

	private Plan.Result<Integer> mutate(String rootResourcePath)
			throws IOException {
		CassandraMutagen mutagen=new CassandraMutagenImpl("Keyspace1");
		mutagen.initialize(rootResourcePath);
		CassandraSubject subject = new CassandraSubject(session, "Keyspace1");
		return mutagen.mutate(subject);
	}

	@Test
	public void testInitializeResourcesByKeyspace() throws Exception {
		Plan.Result<Integer> result = mutate("com/toddfast/mutagen/cassandra/test/mutations/keyspace1");
		State<Integer> state=result.getLastState();

		System.out.println("Mutation complete: "+result.isMutationComplete());
		System.out.println("Exception: "+result.getException());
		if (result.getException()!=null) {
			result.getException().printStackTrace();
		}
		System.out.println("Completed mutations: "+result.getCompletedMutations());
		System.out.println("Remaining mutations: "+result.getRemainingMutations());
		System.out.println("Last state: "+(state!=null ? state.getID() : "null"));

		assertTrue(result.isMutationComplete());
		assertNull(result.getException());
		assertEquals((state!=null ? state.getID() : (Integer)(-1)),(Integer)5);
	}

	@Test
	public void testInitializeResourcesDefaultedKeyspace() throws Exception {
		Plan.Result<Integer> result = mutate("mutations");
		State<Integer> state=result.getLastState();

		System.out.println("Mutation complete: "+result.isMutationComplete());
		System.out.println("Exception: "+result.getException());
		if (result.getException()!=null) {
			result.getException().printStackTrace();
		}
		System.out.println("Completed mutations: "+result.getCompletedMutations());
		System.out.println("Remaining mutations: "+result.getRemainingMutations());
		System.out.println("Last state: "+(state!=null ? state.getID() : "null"));

		assertTrue(result.isMutationComplete());
		assertNull(result.getException());
		assertEquals((state!=null ? state.getID() : (Integer)(-1)),(Integer)4);
	}

	@Test
	public void testData() throws Exception {

		mutate("com/toddfast/mutagen/cassandra/test/mutations/keyspace1");

		Row row1 = session.execute("SELECT * FROM Table1 WHERE key = 'row1';" ).one();

		assertEquals("foo", row1.getString("value1"));
		assertEquals("bar", row1.getString("value2"));

		Row row2 = session.execute("SELECT * FROM Table1 WHERE key = 'row2';" ).one();

		assertEquals("chicken",row2.getString("value1"));
		assertEquals("sneeze",row2.getString("value2"));

		Row row3 = session.execute("SELECT * FROM Table1 WHERE key = 'row3';" ).one();

		assertEquals("bar",row3.getString("value1"));
		assertEquals("baz",row3.getString("value2"));
	}

	private static void defineSession() {
		int maxConnections = 1;
		int maxSimultaneousRequests = 2; 
		
		PoolingOptions pools = new PoolingOptions();
		pools.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 0);
		pools.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, maxSimultaneousRequests);
		pools.setCoreConnectionsPerHost(HostDistance.LOCAL, maxConnections);
		pools.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections);
		pools.setCoreConnectionsPerHost(HostDistance.REMOTE, maxConnections);
		pools.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnections);
		
		Cluster cluster = new Cluster.Builder()
			.withPoolingOptions(pools)
			.addContactPoint("localhost")
			.withPort(9042)
			.withSocketOptions(new SocketOptions().setTcpNoDelay(true))
			.build();
		
		session = cluster.connect();
	}
	
	private static void createKeyspace() {

		System.out.println("Creating keyspace " + session + "...");

		String create_keyspace = 
				"CREATE keyspace IF NOT EXISTS " + KEY_SPACE
				+ " WITH Replication = {'class': 'SimpleStrategy'" +
				", 'replication_factor': '1'};";
		
		// Create the keyspace using CQL
		session.execute(create_keyspace);

		// Mark the new keyspace as in use
		session.execute("use " + KEY_SPACE + ";");
		

		System.out.println("Created keyspace " + KEY_SPACE);
	}

}
