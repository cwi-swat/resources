module Examples

import experiments::resource::Resource;
import IO;

public void runLineStream() {
	registerResource("org.rascalmpl.library.experiments.resource.resources.file.LineStream");
	generateTypedInterface("mapLib", |resource://line-stream/project/Resource/std/Map.rsc|);
	for (l <- mapLib(), /public/ := l) println(l);
}

public void runMySQLTable() {
	registerResource("org.rascalmpl.library.experiments.resource.resources.db.mysql.MySQLTable");
	generateTypedInterface("wikiUsers",|resource://mysql-table/localhost/fsl/wikiuser|,"user","mhills","password","mysqlpwd");
	unames = { un | < _, un, notnull(ue) > <- wikiUsers() };
	println(unames);
}

public void runTwitterSearch() {
	registerResource("org.rascalmpl.library.experiments.resource.resources.twitter.TwitterSearch");
	generateTypedInterface("vvvAmsterdam",|resource://twitter-search|,"query","from:vvvAmsterdam vandaag","lang","nl");
	for (<_,t> <- vvvAmsterdam()) println(t);
}

