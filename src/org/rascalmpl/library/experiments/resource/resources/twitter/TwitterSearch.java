package org.rascalmpl.library.experiments.resource.resources.twitter;

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
import org.rascalmpl.library.experiments.resource.results.TwitterSearchResult;

public class TwitterSearch extends BaseResource {

	private static final String resourceProvider = "twitter-search";

	@Override
	public ResourceResult createResource(IEvaluatorContext ctx, ISourceLocation uri, Type t) {
		// We should have a query
		String queryParams = uri.getURI().getQuery();
		MultiMap<String> params = new MultiMap<String>();
		UrlEncoded.decodeTo(queryParams, params, "UTF-8");
		assert params.containsKey("query");
		return new TwitterSearchResult(t, null, ctx, params.getString("query"), params.getString("lang"), uri, "twitter-search");
	}

	@Override
	public String getProviderString() {
		return TwitterSearch.resourceProvider;
	}

	@Override
	public Type getResourceType(IEvaluatorContext ctx, ISourceLocation uri) {
		TypeFactory tf = TypeFactory.getInstance();
		Type tupleType = tf.tupleType(tf.stringType(), "userId", tf.stringType(), "text");
		Type resultType = tf.listType(tupleType);
		return resultType;
	}

	@Override
	public List<String> getQueryParameters() {
		ArrayList<String> al = new ArrayList<String>();
		al.add("query");
		return al;
	}

	@Override
	public List<Type> getQueryParameterTypes() {
		ArrayList<Type> al = new ArrayList<Type>();
		al.add(TypeFactory.getInstance().stringType());
		return al;
	}

	@Override
	public List<String> getOptionalQueryParameters() {
		ArrayList<String> al = new ArrayList<String>();
		al.add("lang");
		al.add("locale");
		al.add("result_type");
		al.add("until");
		return al;
	}

	@Override
	public List<Type> getOptionalQueryParameterTypes() {
		ArrayList<Type> al = new ArrayList<Type>();
		al.add(makeOptionalParameterType(TypeFactory.getInstance().stringType()));
		al.add(makeOptionalParameterType(TypeFactory.getInstance().stringType()));
		al.add(makeOptionalParameterType(TypeFactory.getInstance().stringType()));
		al.add(makeOptionalParameterType(TypeFactory.getInstance().stringType()));
		return al;
	}

}
