module RSF

import Prelude;
import lang::rsf::IO;
import Resources;
import lang::rascal::types::AbstractType;
import experiments::resource::Resource;

@resource{rsf}
public str generate(str moduleName, loc uri) {
	map[str, type[value]] rels = getRSFTypes(uri);
	
	return  "module <moduleName>
			'import lang::rsf::IO;
			'<for(rname <- rels){>
		    'public <rels[rname]> <rname>() {
			'  return readRSFRelation(#<rels[rname]>, \"<rname>\", <uri>);
			'}<}>";
}