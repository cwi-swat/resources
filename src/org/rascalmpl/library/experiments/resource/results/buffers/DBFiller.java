package org.rascalmpl.library.experiments.resource.results.buffers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.rascalmpl.library.experiments.resource.results.DBHelper;
import org.rascalmpl.values.ValueFactoryFactory;

public class DBFiller implements ILazyFiller {

	private ResultSet rs;
	private Type elementType;
	private TypeStore ts;
	private Connection conn;
	private String queryString;

	public DBFiller(Connection conn, String queryString, Type elementType, TypeStore ts) {
		this.conn = conn;
		this.queryString = queryString;
		this.elementType = elementType;
		this.ts = ts;
	}
	
	@Override
	public IValue[] refill(int pageSize) {
		ArrayList<IValue> al = new ArrayList<IValue>(pageSize);
		int added = 0;
		try {
			while(added < pageSize && rs.next()) {
				++added;
				int columns = elementType.getArity();
				IValue tupleValues[] = new IValue[columns];
				for (int idx = 0; idx < columns; ++idx) {
					tupleValues[idx] = DBHelper.jdbc2pdbValue(rs, idx + 1, elementType.getFieldType(idx), ts);
				}
				al.add(ValueFactoryFactory.getValueFactory().tuple(tupleValues));
				
			}
		} catch (SQLException sqle) {
			
		}
		IValue res[] = new IValue[al.size()];
		for (int idx = 0; idx < al.size(); ++idx) res[idx] = al.get(idx);
		return res;
	}

	private void createResultSet() {
		try {
			Statement stmt = conn.createStatement();
			this.rs = stmt.executeQuery(this.queryString);
		} catch (SQLException sqle) {
			
		}
	}
	@Override
	public ILazyFiller getBufferedFiller() {
		DBFiller dbf = new DBFiller(conn, queryString, elementType, ts);
		dbf.createResultSet();
		return dbf;
	}

}
