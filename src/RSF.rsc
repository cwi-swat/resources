module RSF

import Prelude;
import lang::rsf::IO;
import Resources;
import lang::rascal::types::AbstractType;
import experiments::resource::Resource;

@resource{rsf}
public str generate(str moduleName, loc uri) {
	//if (uri.scheme != "rsf")
	//	throw unexpectedScheme("rsf",head(parts),uri);

	//parts = tail(split("/", uri.path));
	//rsfUri = |<uri.host>:///<intercalate("/", parts)>|;
	
	// Retrieve the relation names and their types
	map[str, Symbol] rsfRels = getRSFTypes(uri);
	
	return  "module <moduleName>
			'import lang::rsf::IO;
			'<for(rname <- rsfRels){>
		         'public <prettyPrintType(rsfRels[rname])> <rname>() {
			     '	return readRSFRelation(#<prettyPrintType(rsfRels[rname])>, \"<rname>\", <rsfUri>);
			     '}
			'<}>
			'";
}