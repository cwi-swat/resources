package org.rascalmpl.library.experiments.resource.results;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.rascalmpl.library.experiments.resource.results.buffers.DBFiller;
import org.rascalmpl.library.experiments.resource.results.buffers.LazyRelation;

public class DBRelation extends LazyRelation {

	private Connection conn;
	private final String queryString;
		
	public DBRelation(Connection conn, String queryString, Type elementType, boolean updateable, TypeStore ts) {
		super(10, new DBFiller(conn, queryString, elementType, ts), elementType);
		this.conn = conn;
		this.queryString = queryString;
	}
	
	@Override
	public boolean isEmpty() {
		boolean res = true;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(queryString);
			if (rs.first()) res = false;
			rs.close();
			stmt.close();
		} catch (SQLException sqle) {
			// TODO: Add a message here...
			sqle.printStackTrace(System.err);
		}
		return res;
	}

	@Override
	public String toString() {
		return "DB-backed Relation [queryString=" + queryString + "]";
	}

	@Override
	public int size() {
		int rowCount = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(queryString);
			if (rs.last()) rowCount = rs.getRow();
			rs.close();
			stmt.close();
		} catch (SQLException sqle) {
			// TODO: Add a message here...
			sqle.printStackTrace(System.err);
		}
		return rowCount;
	}
}
