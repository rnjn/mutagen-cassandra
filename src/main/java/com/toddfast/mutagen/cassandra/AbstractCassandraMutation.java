package com.toddfast.mutagen.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Todd Fast
 */
public abstract class AbstractCassandraMutation implements Mutation<Integer> {
	
	private Keyspace keyspace;

	protected AbstractCassandraMutation(Keyspace keyspace) {
		super();
		this.keyspace=keyspace;
	}
	
	protected Keyspace getKeyspace() {
		return keyspace;
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
		if (getResultingState()!=null) {
			return super.toString()+"[state="+getResultingState().getID()+"]";
		}
		else {
			return super.toString();
		}
	}

	/**
	 * 
	 * @param resourceName
	 * @return
	 */
	protected final State<Integer> parseVersion(String resourceName) {
		String versionString=resourceName;
		int index=versionString.lastIndexOf("/");
		if (index!=-1) {
			versionString=versionString.substring(index+1);
		}

		index=versionString.lastIndexOf(".");
		if (index!=-1) {
			versionString=versionString.substring(0,index);
		}

		StringBuilder buffer=new StringBuilder();
		for (Character c: versionString.toCharArray()) {
			// Skip all initial non-digit characters
			if (!Character.isDigit(c)) {
				if (buffer.length()==0) {
					continue;
				}
				else {
					// End when we reach the first non-digit
					break;
				}
			}
			else {
				buffer.append(c);
			}
		}

		return new SimpleState<Integer>(Integer.parseInt(buffer.toString()));
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
	public final void mutate(Context context)
			throws MutagenException {

		// Perform the mutation
		performMutation(context);

		int version=getResultingState().getID();

		String change=getChangeSummary();
		if (change==null) {
			change="";
		}

		String changeHash=md5String(change);

		// The straightforward way, without locking
		try {
			MutationBatch batch=getKeyspace().prepareMutationBatch();
			batch
				.withRow(CassandraSubject.VERSION_CF,
					CassandraSubject.ROW_KEY)
				.putColumn(CassandraSubject.VERSION_COLUMN,version);

			batch
				.withRow(CassandraSubject.VERSION_CF,
					String.format("%08d",version))
				.putColumn("change",change)
				.putColumn("hash",changeHash);

			batch.execute();
		}
		catch (ConnectionException e) {
			throw new MutagenException("Could not update \"schema_version\" "+
				"column family to state "+version+
				"; schema is now out of sync with recorded version",e);
		}
	}
	
	/**
	 * Returns a md5 string in Hex format
	 *
	 * @param key
	 * @return
	 */
	public static String md5String(String key) {
		
		byte[] theDigest = md5(key);
		
		return Hex.encodeHexString(theDigest);
	}

	/**
	 * Generates an md5 hash
	 *
	 * @param key
	 * @return
	 */
	public static byte[] md5(String key) {
		MessageDigest algorithm;
		try {
			algorithm=MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException(ex);
		}

		algorithm.reset();

		try {
			algorithm.update(key.getBytes("UTF-8"));
		}
		catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}

		byte[] theDigest=algorithm.digest();
		return theDigest;
	}
}
