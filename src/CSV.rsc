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
	map[str,str] options = uri.params;
	
	// We can pass the name of the function to generate. If we did, grab it then remove
	// it from the params, which should just contain those needed by the JDBC driver.
	str funname = "resourceValue";
	if ("funname" in options) {
		funname = options["funname"];
		options = domainX(options,{"funname"});
	}
		
	type[value] csvType = getCSVType(uri, options);
	
	mbody = "module <moduleName>
			'import lang::csv::IO;
			'
			'alias <funname>Type = <csvType>;
			'
			'public <funname>Type <funname>() {
			'	return readCSV(#<funname>Type, <uri>, <options>);
			'}
			'";
 
	return mbody;
}