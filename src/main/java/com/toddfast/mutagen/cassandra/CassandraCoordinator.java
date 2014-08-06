package com.toddfast.mutagen.cassandra;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.Subject;

/**
 *
 *
 * @author Todd Fast
 */
public class CassandraCoordinator implements Coordinator<Integer> {
	
	private CassandraSubject subject;
	
	public CassandraCoordinator(CassandraSubject subject) {
		super();
		if (subject==null) {
			throw new IllegalArgumentException(
				"Parameter \"Subject\" cannot be null");
		}

		this.subject=subject;
	}
	
	public CassandraSubject getSession() {
		return subject;
	}

	/**
	 * Used to insure change versions are monotonically increasing
	 * 
	 */
	@Override
	public boolean accept(Subject<Integer> subject,
			State<Integer> targetState) {
		State<Integer> currentState=subject.getCurrentState();
		return targetState.getID() > currentState.getID();
	}
}
