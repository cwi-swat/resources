package org.rascalmpl.library.experiments.resource.results;

import java.sql.Connection;

import org.eclipse.imp.pdb.facts.ISourceLocation;
import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.rascalmpl.interpreter.IEvaluatorContext;
import org.rascalmpl.interpreter.result.ResourceResult;

public class DBQueryResult extends ResourceResult {

	private String queryText;
	
	public DBQueryResult(Type type, IValue value, IEvaluatorContext ctx, Connection conn, String queryText, ISourceLocation fullURI, String displayURI) {
		super(type, value, ctx, fullURI, displayURI);
		this.queryText = queryText;
		TypeStore ts = ctx.getCurrentEnvt().getStore();
		this.value = new DBRelation(conn, this.queryText, this.type.getElementType(), false, ts);
	}
	
}
