module Twitter

import Resources;
import Type;
import IO;
import ParseTree;
import Exception;
import List;
import lang::json::syntax::JSON;
import lang::json::ast::JSON;
import lang::json::ast::Implode;

//@resource{twitter}
//public str generate(str moduleName, loc uri) {
//	if (uri.scheme != "twitter")
//		throw unexpectedScheme("twitter",head(parts),uri);
//
//	parts = tail(split("/", uri.path));
//	csvUri = |<uri.host>:///<intercalate("/",parts)>|;
//	println("Computed URI: <csvUri>");
//	
//	map[str,str] options = ( );
//	for (qp <- split("&", uri.query)) {
//		ops = split("=",qp);
//		if (size(ops) == 2)
//			options[uriDecode(ops[0])] = uriDecode(ops[1]);
//		else if (size(ops) > 2)
//			throw "Unexpected option"; // todo: replace with an exception constructor
//	}
//	println("Computed options: <options>");
//		
//	Symbol csvType = getCSVType(csvUri, options);
//	
//	mbody = "module <moduleName>
//			'import lang::csv::IO;
//			'public <prettyPrintType(csvType)> resourceValue() {
//			'	return readCSV(#<prettyPrintType(csvType)>, <csvUri>, <options>);
//			'}
//			'";
//			
//	return mbody;
//}

public list[Value] doQuery(int maxResults, str query) {
	loc l = |http://search.twitter.com/search.json?rpp=100&q=<query>&lang=en|;
	bool keepGoing = true;
	list[Value] results = [ ];
	while (keepGoing) {
		try {
			str sres = readFile(l);
			Value res = buildAST(parse(#start[JSONText],sres));
			if (object(_) := res) {
				if ("results" in res.members && array(vs) := res.members["results"])
					results += vs;
				if ("next_page" in res.members && string(s) := res.members["next_page"]) {
					println(s);
					l = |http://search.twitter.com/search.json<s>|;
				} else {
					keepGoing = false;
				}
			}
		} catch IO(msg) : {
			println("Read failed, stopping iteration: <msg>");
			keepGoing = false;
		}
		
		if (size(results) >= maxResults) keepGoing = false;
	}
	return results;		
}