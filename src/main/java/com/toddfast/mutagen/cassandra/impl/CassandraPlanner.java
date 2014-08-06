package com.toddfast.mutagen.cassandra.impl;

import com.toddfast.mutagen.Coordinator;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Subject;
import com.toddfast.mutagen.basic.BasicPlanner;
import com.toddfast.mutagen.cassandra.CassandraSubject;
import com.toddfast.mutagen.cassandra.mutation.CQLMutation;
import com.toddfast.mutagen.cassandra.mutation.CSVMutation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.toddfast.mutagen.cassandra.mutation.ClassMutation.loadMutationClass;

/**
 *
 * @author Todd Fast
 */
public class CassandraPlanner extends BasicPlanner<Integer> {

	protected CassandraPlanner(CassandraSubject subject, 
			List<String> mutationResources) {
		super(loadMutations(subject,mutationResources),null);
	}


	private static List<Mutation<Integer>> loadMutations(
			CassandraSubject subject, Collection<String> resources) {

		List<Mutation<Integer>> result=new ArrayList<Mutation<Integer>>();

		for (String resource: resources) {

			// Allow .sql files because some editors have syntax highlighting
			// for SQL but not CQL
			if (resource.endsWith(".cql") || resource.endsWith(".sql")) {
				result.add(new CQLMutation(subject,resource));
			} else if (resource.endsWith(".csv")) {
				result.add(new CSVMutation(subject, resource));
			} else if (resource.endsWith(".class")) {
				result.add(loadMutationClass(subject,resource));
			}
			else {
				throw new IllegalArgumentException("Unknown type for "+
					"mutation resource \""+resource+"\"");
			}
		}

		return result;
	}

	@Override
	protected Mutation.Context createContext(Subject<Integer> subject,
		Coordinator<Integer> coordinator) {
		return new CassandraContext(subject,coordinator);
	}

	@Override
	public Plan<Integer> getPlan(Subject<Integer> subject,
			Coordinator<Integer> coordinator) {
		return super.getPlan(subject,coordinator);
	}
}
