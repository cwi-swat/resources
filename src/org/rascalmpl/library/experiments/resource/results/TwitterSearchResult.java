package org.rascalmpl.library.experiments.resource.results;

import org.eclipse.imp.pdb.facts.ISourceLocation;
import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.rascalmpl.interpreter.IEvaluatorContext;
import org.rascalmpl.interpreter.result.ResourceResult;
import org.rascalmpl.library.experiments.resource.results.buffers.LazyList;
import org.rascalmpl.library.experiments.resource.results.buffers.TwitterSearchFiller;

public class TwitterSearchResult extends ResourceResult {

	public TwitterSearchResult(Type type, IValue value, IEvaluatorContext ctx, String queryString, String lang, ISourceLocation fullURI, String displayURI) {
		super(type, value, ctx, fullURI, displayURI);
		TypeStore ts = ctx.getCurrentEnvt().getStore();

//		// Create all the types used in this value. This includes the type of the underlying table, as a relation; the type of the
//		// overall result, which is an instantiated parametric ADT; and the type of the constructor.
//		TypeFactory tf = TypeFactory.getInstance();
//		Type paramType = tf.parameterType("T");
//		HashMap<Type,Type> bindings = new HashMap<Type,Type>();
//		bindings.put(paramType, t);
//
//		Type adtType = tf.abstractDataType(ts, "Resource", paramType);
//		Type consTuple = tf.tupleType(paramType, "searchResults");
//		Type consType = tf.constructorFromTuple(ts, adtType, "twitterSearch", consTuple);
//
//		consType = consType.instantiate(bindings);
//
//		// Create the stored values. This includes the overall value, which is of a constructor type, as well as the
//		// inner type, which is a special relation used to hold database results.
//		this.type = consType.getAbstractDataType();
//		this.value = ValueFactoryFactory.getValueFactory().constructor(consType, new BufferedList(25, new TwitterFillerFactory(queryString, ts),t.getElementType()));
		
		this.value = new LazyList(25, new TwitterSearchFiller(queryString, lang, ts),this.type.getElementType());
	}
}
