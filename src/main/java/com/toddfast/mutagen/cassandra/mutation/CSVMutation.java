package com.toddfast.mutagen.cassandra.mutation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.jboss.netty.util.internal.StringUtil;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.toddfast.mutagen.MutagenException;
import com.toddfast.mutagen.State;

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
	
	private StringBuilder source = new StringBuilder();
	
	private State<Integer> state;
	
	private List<Statement> updateStatements;
	
	private static final String CSV_DELIM = ",";

	public CSVMutation(Session session, String resourceName) {
		super(session);
		this.state=super.parseVersion(resourceName);
		this.updateStatements = new ArrayList<Statement>();
		loadCSVData(resourceName);
	}

	@Override
	public State<Integer> getResultingState() {
		return state;
	}

	@Override
	protected String getChangeSummary() {
		return source.toString();
	}

	@Override
	protected void performMutation(com.toddfast.mutagen.Mutation.Context context) {
		
		context.debug("Executing mutation {}",state.getID());
		
		for(Statement statement : updateStatements) {
			try {
				ResultSet result = getSession().execute(statement);
			} catch (Exception e) {
				context.error("Exception executing update from CSV\"{}\"", e);
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
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
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
		
		// TODO: Parse this from the data file
		String tableName = "Test1";
		
		StringBuilder updateDataCQL = new StringBuilder();
		updateDataCQL.append("UPDATE ").append(tableName).append(" SET ");
		// first column is the key
		for(int i = 1; i < columnNames.length; i++) {
			updateDataCQL.append(columnNames[i].toCharArray()).append("=").append(rowValues[i]);
			if(i != (columnNames.length - 1)) {
				updateDataCQL.append(",");
			}
		}
		updateDataCQL.append(" WHERE ").append(columnNames[0].toCharArray()).append("=").append(rowValues[0]).append(";");
			
		Statement statement = new SimpleStatement(updateDataCQL.toString());
			
		source.append(StringUtil.NEWLINE).append(updateDataCQL.toString());
			
		updateStatements.add(statement);
	}
}
