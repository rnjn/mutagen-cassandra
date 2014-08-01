package com.toddfast.mutagen.cassandra.mutation;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.CassandraSubject;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;

/**
 *
 * @author Todd Fast
 */
public abstract class AbstractCassandraMutation implements Mutation<Integer> {

	private Session session;

	protected AbstractCassandraMutation(Session session) {
		super();
		this.session = session;
	}

	protected Session getSession() {
		return session;
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
		String versionString = resourceName;
		int index = versionString.lastIndexOf("/");
		if (index != -1) {
			versionString = versionString.substring(index + 1);
		}

		index = versionString.lastIndexOf(".");
		if (index != -1) {
			versionString = versionString.substring(0, index);
		}

		StringBuilder buffer = new StringBuilder();
		for (Character c : versionString.toCharArray()) {
			// Skip all initial non-digit characters
			if (!Character.isDigit(c)) {
				if (buffer.length() == 0) {
					continue;
				} else {
					// End when we reach the first non-digit
					break;
				}
			} else {
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
	public final void mutate(Context context) throws MutagenException {

		// Perform the mutation
		performMutation(context);

		int version = getResultingState().getID();

		String change = getChangeSummary();
		if (change == null) {
			change = "";
		}

		String changeHash = md5String(change);

		String query = "INSERT INTO schema_version (version, change, hash) VALUES (?,?,?)";
		PreparedStatement prepare = getSession().prepare(query);
		BoundStatement boundStatement = prepare.bind(version, change,
				changeHash);

		try {
			getSession().execute(boundStatement);
		} catch (Exception e) {
			throw new MutagenException("Could not update \"schema_version\" "
					+ "column family to state " + version
					+ "; schema is now out of sync with recorded version", e);
		}

		query = "UPDATE schema_current_version set version = ? where state = ?";
		prepare = getSession().prepare(query);
		boundStatement = prepare.bind(version, CassandraSubject.ROW_KEY);

		try {
			getSession().execute(boundStatement);
		} catch (Exception e) {
			throw new MutagenException("Could not update \"schema_version\" "
					+ "column family to state " + version
					+ "; schema is now out of sync with recorded version", e);
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
			algorithm = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException ex) {
			throw new RuntimeException(ex);
		}

		algorithm.reset();

		try {
			algorithm.update(key.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException ex) {
			throw new RuntimeException(ex);
		}

		byte[] theDigest = algorithm.digest();
		return theDigest;
	}
}
