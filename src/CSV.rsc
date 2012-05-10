module CSV

import Resources;
import Type;
import List;
import String;
import Exception;
import lang::rascal::types::AbstractType;
import experiments::resource::Resource;
import IO;
import lang::csv::IO;

@resource{csv}
public str generate(str moduleName, loc uri) {
	if (uri.scheme != "csv")
		throw unexpectedScheme("csv",head(parts),uri);

	parts = tail(split("/", uri.path));
	csvUri = |<uri.host>:///<intercalate("/",parts)>|;
	println("Computed URI: <csvUri>");
	
	map[str,str] options = ( );
	for (qp <- split("&", uri.query)) {
		ops = split("=",qp);
		if (size(ops) == 2)
			options[uriDecode(ops[0])] = uriDecode(ops[1]);
		else if (size(ops) > 2)
			throw "Unexpected option"; // todo: replace with an exception constructor
	}
	println("Computed options: <options>");
		
	Symbol csvType = getCSVType(csvUri, options);
	
	mbody = "module <moduleName>
			'import lang::csv::IO;
			'public <prettyPrintType(csvType)> resourceValue() {
			'	return readCSV(#<prettyPrintType(csvType)>, <csvUri>, <options>);
			'}
			'";
			
	return mbody;
}