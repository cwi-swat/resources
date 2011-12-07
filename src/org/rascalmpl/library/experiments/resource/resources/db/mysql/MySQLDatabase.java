package org.rascalmpl.library.experiments.resource.resources.db.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.imp.pdb.facts.ISourceLocation;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeFactory;
import org.eclipse.jetty.util.MultiMap;
import org.eclipse.jetty.util.UrlEncoded;
import org.rascalmpl.interpreter.IEvaluatorContext;
import org.rascalmpl.interpreter.result.ResourceResult;
import org.rascalmpl.library.experiments.resource.resources.BaseResource;
import org.rascalmpl.library.experiments.resource.results.DBResult;

public class MySQLDatabase extends BaseResource {
	
	private static final String resourceProvider = "mysql-database";
	
	@Override
	public ResourceResult createResource(IEvaluatorContext ctx, ISourceLocation uri, Type t) {
		String path = uri.getURI().getPath();
		path = path.substring(1); //remove leading / 
		String parts[] = path.split("[/]");
		String host = parts[0];
		String dbName = parts[1];

		MultiMap<String> params = new MultiMap<String>();
		UrlEncoded.decodeTo(uri.getURI().getQuery(), params, "UTF-8");
		assert params.containsKey("user");
		assert params.containsKey("password");
		
		String connectString = MySQLConstants.connectPrefix + host + "/" + dbName + "?user=" + params.getString("user") + "&password=" + params.getString("password");
		try {
			Class.forName(MySQLConstants.mysqlDriver);
			Connection conn =  DriverManager.getConnection(connectString);
			return new DBResult(t, null, ctx, conn, dbName, uri, "mysql-database/"+host+"/"+dbName);
		} catch (SQLException sqle) {
			
		} catch (ClassNotFoundException cnfe) {
			
		}
			
		return null;
	}

	@Override
	public String getProviderString() {
		return MySQLDatabase.resourceProvider;
	}

	@Override
	public Type getResourceType(IEvaluatorContext ctx, ISourceLocation uri) {
		TypeFactory tf = TypeFactory.getInstance();
		return tf.tupleType(tf.stringType(), "dbName", tf.setType(tf.stringType()), "tables", tf.setType(tf.stringType()), "views");
	}

	@Override
	public List<String> getPathItems() {
		ArrayList<String> al = new ArrayList<String>();
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
