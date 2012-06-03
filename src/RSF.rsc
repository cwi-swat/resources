module RSF

import Resources;
import Type;
import List;
import String;
import Exception;
import lang::rascal::types::AbstractType;
import experiments::resource::Resource;
import IO;
import Map;
import lang::rsf::IO;

@resource{rsf}
public str generate(str moduleName, loc uri) {
	if (uri.scheme != "rsf")
		throw unexpectedScheme("rsf",head(parts),uri);

	parts = tail(split("/", uri.path));
	rsfUri = |<uri.host>:///<intercalate("/",parts)>|;
	println("Computed URI: <rsfUri>");
	
	map[str,str] options = ( );
	for (qp <- split("&", uri.query)) {
		ops = split("=",qp);
		if (size(ops) == 2)
			options[uriDecode(ops[0])] = uriDecode(ops[1]);
		else if (size(ops) > 2)
			throw "Unexpected option"; // todo: replace with an exception constructor
	}

	// We can pass the name of the function to generate. If we did, grab it then remove
	// it from the params, which should just contain those needed by the JDBC driver.
	str funname = "resourceValue";
	if ("funname" in options) {
		funname = options["funname"];
		options = domainX(options,{"funname"});
	}
		
	map[str, Symbol] rsfRels = getRSFTypes(rsfUri);
	
	mbody = "module <moduleName>
			'import lang::rsf::IO;
			'<for(rname <- rsfRels){>
		         'public <prettyPrintType(rsfRels[rname])> <rname>() {
			     '	return readRSF(#<prettyPrintType(rsfRels[rname])>, <rname>, <rsfUri>, <options>);
			     '}
			'<}>
			'";
    println(mbody);
	return mbody;
}