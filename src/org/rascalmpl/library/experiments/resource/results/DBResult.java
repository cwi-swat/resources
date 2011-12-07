package org.rascalmpl.library.experiments.resource.results;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.eclipse.imp.pdb.facts.ISet;
import org.eclipse.imp.pdb.facts.ISetWriter;
import org.eclipse.imp.pdb.facts.ISourceLocation;
import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeFactory;
import org.rascalmpl.interpreter.IEvaluatorContext;
import org.rascalmpl.interpreter.result.ResourceResult;
import org.rascalmpl.interpreter.staticErrors.UnsupportedOperationError;
import org.rascalmpl.values.ValueFactoryFactory;

public class DBResult extends ResourceResult {

	public DBResult(Type type, IValue value, IEvaluatorContext ctx, Connection conn, String dbName, ISourceLocation fullURI, String displayURI) {
		super(type, value, ctx, fullURI, displayURI);
		
		try {
			DatabaseMetaData dmd = conn.getMetaData();

			String findTables[] = { "TABLE" };
			ResultSet rs = dmd.getTables(null, null, null, findTables);
			ISetWriter sw = ValueFactoryFactory.getValueFactory().setWriter(TypeFactory.getInstance().stringType());
			while (rs.next()) {
				sw.insert(ValueFactoryFactory.getValueFactory().string(rs.getString("TABLE_NAME")));
			}
			rs.close();
			ISet tableSet = sw.done();
			
			String findViews[] = { "VIEW" };
			rs = dmd.getTables(null, null, null, findViews);
			sw = ValueFactoryFactory.getValueFactory().setWriter(TypeFactory.getInstance().stringType());
			while (rs.next()) {
				sw.insert(ValueFactoryFactory.getValueFactory().string(rs.getString("TABLE_NAME")));
			}
			rs.close();
			ISet viewSet = sw.done();
			this.value = ValueFactoryFactory.getValueFactory().tuple(ValueFactoryFactory.getValueFactory().string(dbName), tableSet, viewSet);
		} catch (SQLException e) {
			throw new UnsupportedOperationError("Could not create table-based relation: " + e.toString(), null);
		}
	}
}
