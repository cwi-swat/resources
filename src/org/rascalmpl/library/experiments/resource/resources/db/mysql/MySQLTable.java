package org.rascalmpl.library.experiments.resource.resources.db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.imp.pdb.facts.ISourceLocation;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeFactory;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.eclipse.jetty.util.MultiMap;
import org.eclipse.jetty.util.UrlEncoded;
import org.rascalmpl.interpreter.IEvaluatorContext;
import org.rascalmpl.interpreter.result.ResourceResult;
import org.rascalmpl.library.experiments.resource.resources.BaseResource;
import org.rascalmpl.library.experiments.resource.results.DBHelper;
import org.rascalmpl.library.experiments.resource.results.DBTableResult;

public class MySQLTable extends BaseResource {

	private static final String resourceProvider = "mysql-table";

	@Override
	public ResourceResult createResource(IEvaluatorContext ctx, ISourceLocation uri, Type t) {
		String path = uri.getURI().getPath();
		path = path.substring(1); //remove leading / 
		String parts[] = path.split("[/]");
		String host = parts[0];
		String dbName = parts[1];
		String tableName = parts[2];

		// We should have been given both the user name and the password
		// so we can actually connect to the database to get the table.
		String queryParams = uri.getURI().getQuery();
		MultiMap<String> params = new MultiMap<String>();
		UrlEncoded.decodeTo(queryParams, params, "UTF-8");
		assert params.containsKey("user");
		assert params.containsKey("password");

		String connectString = MySQLConstants.connectPrefix + host + "/" + dbName + "?user=" + params.getString("user") + "&password=" + params.getString("password");
		try {
			Class.forName(MySQLConstants.mysqlDriver);
			Connection conn =  DriverManager.getConnection(connectString);
			return new DBTableResult(t, null, ctx, conn, tableName, uri, "mysql-table/"+host+"/"+dbName+"/"+tableName);
		} catch (SQLException sqle) {
			// TODO: Should throw an exception here
		} catch (ClassNotFoundException cnfe) {
			// TODO: Should throw an exception here
		}
			
		return null;
	}

	@Override
	public String getProviderString() {
		return MySQLTable.resourceProvider;
	}

	@Override
	public Type getResourceType(IEvaluatorContext ctx, ISourceLocation uri) {
		String path = uri.getURI().getPath();
		path = path.substring(1); //remove leading / 
		String parts[] = path.split("[/]");
		String host = parts[0];
		String dbName = parts[1];
		String tableName = parts[2];

		// We should have been given both the user name and the password
		// so we can actually connect to the database to get the table.
		String queryParams = uri.getURI().getQuery();
		MultiMap<String> params = new MultiMap<String>();
		UrlEncoded.decodeTo(queryParams, params, "UTF-8");
		assert params.containsKey("user");
		assert params.containsKey("password");

		String connectString = MySQLConstants.connectPrefix + host + "/" + dbName + "?user=" + params.getString("user") + "&password=" + params.getString("password");
		try {
			Class.forName(MySQLConstants.mysqlDriver);
			Connection conn =  DriverManager.getConnection(connectString);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
			ResultSetMetaData rsm = rs.getMetaData();
			String columnNames[] = new String[rsm.getColumnCount()];
			Type columnTypes[] = new Type[rsm.getColumnCount()];
			TypeStore ts = ctx.getCurrentEnvt().getStore();
			for (int idx = 1; idx <= rsm.getColumnCount(); ++idx) {
				columnNames[idx-1] = rsm.getColumnLabel(idx);
				columnTypes[idx-1] = DBHelper.jdbc2pdbType(rsm.getColumnType(idx), rsm.isNullable(idx) != ResultSetMetaData.columnNoNulls, ts);
			}
			rs.close();
			stmt.close();
			conn.close();
			
			// Create all the types used in this value. This includes the type of the underlying table, as a relation; the type of the
			// overall result, which is an instantiated parametric ADT; and the type of the constructor.
			TypeFactory tf = TypeFactory.getInstance();
			return tf.relTypeFromTuple(tf.tupleType(columnTypes, columnNames));
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		} catch (ClassNotFoundException cnfe) {
			// TODO: Should throw an exception here
		}
			
		return null;
	}

	@Override
	public List<String> getPathItems() {
		ArrayList<String> al = new ArrayList<String>();
		al.add("host");
		al.add("database");
		al.add("table");
		return al;
	}

	@Override
	public List<String> getQueryParameters() {
		ArrayList<String> al = new ArrayList<String>();
		al.add("user");
		al.add("password");
		return al;
	}

	@Override
	public List<Type> getQueryParameterTypes() {
		ArrayList<Type> al = new ArrayList<Type>();
		al.add(TypeFactory.getInstance().stringType());
		al.add(TypeFactory.getInstance().stringType());
		return al;
	}
}
