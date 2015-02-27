package com.toddfast.mutagen.cassandra.mutation;

import java.io.File;

import com.toddfast.mutagen.MutagenException;

public class MutationParser {
	
	private static final int SUBJECT_INDEX = 0;
	
	private static final int VERSION_INDEX = 1;
	
	public static Integer parseMutationVersion(String resourcePath) {
		String[] fileNameParts = parseFilename(resourcePath);
		if(1 == fileNameParts.length) return Integer.parseInt(fileNameParts[0]);
		return Integer.parseInt(fileNameParts[VERSION_INDEX]);
	}
	
	public static String parseMutationSubject(String resourcePath, String defaultKeyspace) {

		String[] fileNameParts = parseFilename(resourcePath);
		if(fileNameParts.length < 2) {
			return defaultKeyspace;
		}
		return fileNameParts[SUBJECT_INDEX];
	}
	
	private static String[] parseFilename(String resourcePath) {
		return new File(resourcePath).getName().split("\\.")[0].split("_");
	}
}
