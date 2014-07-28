package com.toddfast.mutagen.cassandra.impl;

//import com.conga.nu.AllowField;
//import com.conga.nu.Scope;
//import com.conga.nu.ServiceProvider;
import com.netflix.astyanax.Keyspace;
import com.toddfast.mutagen.Mutation;
import com.toddfast.mutagen.Plan;
import com.toddfast.mutagen.Planner;
import com.toddfast.mutagen.basic.ResourceScanner;
import com.toddfast.mutagen.cassandra.CassandraCoordinator;
import com.toddfast.mutagen.cassandra.CassandraMutagen;
import com.toddfast.mutagen.cassandra.CassandraSubject;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.commons.io.FilenameUtils;

/**
 *
 * 
 * @author Todd Fast
 */
//@ServiceProvider(scope=Scope.CLIENT_MANAGED)
public class CassandraMutagenImpl implements CassandraMutagen {
	
	private List<String> resources = new ArrayList<String>();
	
	
	/**
	 * List contains resources in sorted order of version
	 *
	 */
	public List<String> getResources() {
		return resources;
	}
	
	/**
	 * Find all cassandra version files given a root
	 * resource path to search.
	 */
	@Override
	public void initialize(String rootResourcePath)
			throws IOException {

		try {
			List<String> discoveredResources=
				ResourceScanner.getInstance().getResources(
					rootResourcePath,Pattern.compile(".*"),
					getClass().getClassLoader());

			// Make sure we found some resources
			if (discoveredResources.isEmpty()) {
				throw new IllegalArgumentException("Could not find resources "+
					"on path \""+rootResourcePath+"\"");
			}

			Collections.sort(discoveredResources,COMPARATOR);

			for (String resource: discoveredResources) {
				System.out.println("Found mutation resource \""+resource+"\"");

				if (resource.endsWith(".class")) {
					// Framework depends on unix style file paths for proper
					// class loading.
					resource = FilenameUtils.separatorsToUnix(resource);
					resource=resource.substring(
						resource.indexOf(rootResourcePath));
					if (resource.contains("$")) {
						// skip inner classes
						continue;
					}
                }
				resources.add(resource);
			}
		}
		catch (URISyntaxException e) {
			throw new IllegalArgumentException("Could not find resources on "+
				"path \""+rootResourcePath+"\"",e);
		}
	}

	@Override
	public Plan.Result<Integer> mutate(Keyspace keyspace) {
		// Do this in a VM-wide critical section. External cluster-wide 
		// synchronization is going to have to happen in the coordinator.
		synchronized (System.class) {
			CassandraCoordinator coordinator=new CassandraCoordinator(keyspace);
			CassandraSubject subject=new CassandraSubject(keyspace);

			Planner<Integer> planner=
				new CassandraPlanner(keyspace,getResources());
			Plan<Integer> plan=planner.getPlan(subject,coordinator);

			// Execute the plan
			Plan.Result<Integer> result=plan.execute();
			return result;
		}
	}

	/**
	 * Sorts by root file name, ignoring path and file extension
	 *
	 */
	private static final Comparator<String> COMPARATOR=
		new Comparator<String>() {
			@Override
			public int compare(String path1, String path2) {
				File file1 = new File(path1);
				File file2 = new File(path2);
				
				String filename1 = file1.getName();
				String filename2 = file2.getName();
				return filename1.compareTo(filename2);
				
			}
		};
}
