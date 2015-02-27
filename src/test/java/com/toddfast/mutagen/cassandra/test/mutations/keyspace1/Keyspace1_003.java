package com.toddfast.mutagen.cassandra.test.mutations.keyspace1;

import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.basic.SimpleState;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.mutation.AbstractCassandraMutation;

/**
 *
 * @author Todd Fast
 */
public class Keyspace1_003 extends AbstractCassandraMutation {
	

	private State<Integer> state;

	public Keyspace1_003(CassandraSubject subject) {
		super(subject);
		state=new SimpleState<Integer>(3);
	}


	@Override
	public State<Integer> getResultingState() {
		return state;
	}



	/**
	 * Return a canonical representative of the change in string form
	 *
	 */
	@Override
	protected String getChangeSummary() {
		return "update Table1 set value1='chicken', value2='sneeze' "+
			"where key='row2';";
	}

	@Override
	protected void performMutation(Context context) {
		context.debug("Executing mutation {}",state.getID());
		
		try {
			getSubject().getSession().execute(getChangeSummary());
		} catch (Exception e) {
			throw new MutagenException("Could not update table Table1", e);
		}
	} 
}
