module CSV

import Resources;
import Type;
import List;
import String;
import Exception;
import lang::rascal::types::AbstractType;
import experiments::resource::Resource;
import IO;
import Map;
import lang::csv::IO;

@resource{csv}
public str generate(str moduleName, loc uri) {
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
		
	Symbol csvType = getCSVType(uri, options);
	
	mbody = "module <moduleName>
			'import lang::csv::IO;
			'
			'alias <funname>Type = <prettyPrintType(csvType)>;
			'
			'public <funname>Type <funname>() {
			'	return readCSV(#<funname>Type, <uri>, <options>);
			'}
			'";
    println("Generates: <mbody>");
	return mbody;
}