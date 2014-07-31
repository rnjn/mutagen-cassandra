package com.toddfast.mutagen.cassandra.mutation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.cql.CqlStatementResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;
import com.toddfast.mutagen.cassandra.CassandraSubject;

/**
 * This class handles the parsing of CSV files that contain
 * seed data for Column Family (table) in a specific Keyspace.
 * 
 * Filename Format: 	V{number}_{KeyspaceName}_{ColumnFamily}
 * Filename Example:	V0043_MusicApp_AblumsByArtist
 * 
 * CSV Format:
 * 	Header: 	{rowKey}, {columnName},...,{columnName}
 * 	Example:	artistName, albumName, yearReleased, numberOfTracks
 * 				"Pink Floyd",  "The Wall", "1979", "26"
 * 
 * @author Andrew From
 */
public class CSVMutation extends AbstractCassandraMutation {
	
	private String source;
	
	private State<Integer> state;
	
	List<MutationBatch> batchWrites = new ArrayList<MutationBatch>();
	
	final ColumnFamily<String,String> CF_TEST1=
			ColumnFamily.newColumnFamily("Test1",
				StringSerializer.get(),StringSerializer.get());
	
	private static final String CSV_DELIM = ",";

	public CSVMutation(Keyspace keyspace, String resourceName) {
		super(keyspace);
		this.state=super.parseVersion(resourceName);
		loadCSVData(resourceName);
	}

	@Override
	public State<Integer> getResultingState() {
		return state;
	}

	@Override
	protected String getChangeSummary() {
		return source;
	}

	@Override
	protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {
		context.debug("Executing mutation {}",state.getID());
		for(MutationBatch mutation : batchWrites) {
			try {
				OperationResult<Void> result = mutation.execute();
				
				context.info("Successfully executed CQL \"{}\" in {} attempts",
						result.getAttemptsCount());
			} catch (ConnectionException e) {
				context.error("Exception executing update from CSV\"{}\"",mutation.getRowKeys().toString(), e);
				throw new MutagenException("Exception executing update from csv\"",e);
			}
		}
	}
	
	private void loadCSVData(String resourceName) {
		BufferedReader br = null;
		String currentLine = null;
		String[] columnNames = null;
		
		try {
			br = new BufferedReader(new FileReader(resourceName));
			
			if((currentLine = br.readLine()) != null) {
				columnNames = currentLine.split(CSV_DELIM);
			}
			
			while((currentLine = br.readLine()) != null) {
				String[] rowValues = currentLine.split(CSV_DELIM);
				addBatchWrite(columnNames, rowValues);
			}
			
		} catch (FileNotFoundException e) {
			
		} catch (IOException e) {
			
		} finally {
			if(br != null) {
				try{
					br.close();
				}catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	private void addBatchWrite(String[] columnNames, String[] rowValues) {
		MutationBatch m = getKeyspace().prepareMutationBatch();
		
		// first column is the key
		for(int i = 1; i < columnNames.length; i++) {
			m.withRow(CF_TEST1, rowValues[0])
				.putColumn(columnNames[i], rowValues[i], null);
		}
		
		this.batchWrites.add(m);
	}
}
