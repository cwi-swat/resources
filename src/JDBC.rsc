module JDBC

import Exception;
import Type;
import Resources;
import Map;
import String;
import List;
import Set;
import IO;
import lang::rascal::types::AbstractType;
import experiments::resource::Resource;

@doc{Given the name of a JDBC driver class, register it so it can be used in connections.}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java void registerJDBCClass(str className);

@doc{The JDBC driver name for MySQL}
public str mysqlDriver = "com.mysql.jdbc.Driver";

@doc{Generate a MySQL connect string.}
public str mysqlConnectString(list[tuple[str,str]] properties) {
	hosts = toSet(properties)["host"];
	dbs = toSet(properties)["database"];
	
	if (size(hosts) > 1) throw "Only one host can be specified";
	host = (size(hosts) == 0) ? "127.0.0.1" : getOneFrom(hosts);
	
	if (size(dbs) > 1) throw "Only one database can be specified";
	if (size(dbs) < 1) throw "A database must be specified";
	database = getOneFrom(dbs);
	 
	return "jdbc:mysql://<host>/<database>?<intercalate("&",["<x>=<y>"|<x,y> <- properties, x notin {"host","database"}])>";
}

@doc{Indicates which drivers are needed for the different database types encoded in URIs}
public map[str,str] drivers = ( "mysql" : mysqlDriver );

@doc{Generates connect strings for different database types}
public map[str,str(list[tuple[str,str]])] connectStringGenerators = ( "mysql" : mysqlConnectString );

@doc{JDBC Connection type}
data Connection = jdbcConnection(int id);

@doc{Create a connection based on the given connection string.}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java Connection createConnection(str connectString);

@doc{Close the given connection.}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java void closeConnection(Connection connection);

@doc{Get the types of tables available through this connection.}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java list[str] getTableTypes(Connection connection);

@doc{The JDBC types that could be assigned to various columns.}
data JDBCType
	= array()
	| bigInt()
	| binary()
	| bit()
	| blob()
	| boolean()
	| char()
	| clob()
	| dataLink()
	| date()
	| decimal()
	| distinct()
	| double()
	| float()
	| integer()
	| javaObject()
	| longNVarChar()
	| longVarBinary()
	| longVarChar()
	| nChar()
	| nClob()
	| null()
	| numeric()
	| nVarChar()
	| other()
	| \real()
	| ref()
	| rowId()
	| smallInt()
	| sqlXML()
	| struct()
	| time()
	| timeStamp()
	| tinyInt()
	| varBinary()
	| varChar()
	;

@doc{A column in a table or view}	
data Column = column(str columnName, JDBCType columnType, bool nullable);

@doc{A table in a database}
data Table = table(str tableName, list[Column] columns);

@doc{Get the tables visible through this connection (just names).}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java set[str] getTableNames(Connection connection);

@doc{Get the tables visible through this connection (with column info).}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java set[Table] getTables(Connection connection);

@doc{Get the tables visible through this connection (with column info).}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java set[Table] getViews(Connection connection);

@doc{Get the Table metadata for a named table.}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java Table getTable(Connection connection, str tableName);

@doc{Get the Table metadata for a named view.}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public Table getView(Connection connection, str viewName) = getTable(connection, viewName);

@doc{An exception thrown when we try to translate (or otherwise use) a JDBC type with no Rascal equivalent.}
data RuntimeException = unsupportedJDBCType(JDBCType jdbcType);

@doc{Get the Rascal type (as a symbol) for the given JDBC type}
public Symbol jdbc2RascalType(array()) { throw unsupportedJDBCType(array()); } 
public Symbol jdbc2RascalType(bigInt()) = \int();
public Symbol jdbc2RascalType(binary()) = \list(\int());
public Symbol jdbc2RascalType(bit()) = \bool();
public Symbol jdbc2RascalType(blob()) = \list(\int());
public Symbol jdbc2RascalType(boolean()) = \bool();
public Symbol jdbc2RascalType(char()) = \str();
public Symbol jdbc2RascalType(clob()) = \str();
public Symbol jdbc2RascalType(dataLink()) { throw unsupportedJDBCType(datalink()); }
public Symbol jdbc2RascalType(date()) = \datetime();
public Symbol jdbc2RascalType(decimal()) = \real();
public Symbol jdbc2RascalType(distinct()) { throw unsupportedJDBCType(distinct()); }
public Symbol jdbc2RascalType(double()) = \real();
public Symbol jdbc2RascalType(float()) = \real();
public Symbol jdbc2RascalType(integer()) = \int();
public Symbol jdbc2RascalType(javaObject()) { throw unsupportedJDBCType(javaObject()); }
public Symbol jdbc2RascalType(longNVarChar()) = \str();
public Symbol jdbc2RascalType(longVarBinary()) = \list(\int());
public Symbol jdbc2RascalType(longVarChar()) = \str();
public Symbol jdbc2RascalType(nChar()) = \str();
public Symbol jdbc2RascalType(nClob()) = \str();
public Symbol jdbc2RascalType(null()) { throw unsupportedJDBCType(null()); }
public Symbol jdbc2RascalType(numeric()) = \real();
public Symbol jdbc2RascalType(nVarChar()) = \string();
public Symbol jdbc2RascalType(other()) { throw unsupportedJDBCType(other()); }
public Symbol jdbc2RascalType(\real()) = \real();
public Symbol jdbc2RascalType(ref()) { throw unsupportedJDBCType(ref()); }
public Symbol jdbc2RascalType(rowId()) { throw unsupportedJDBCType(rowId()); }
public Symbol jdbc2RascalType(smallInt()) = \int();
public Symbol jdbc2RascalType(sqlXML()) { throw unsupportedJDBCType(sqlXML()); }
public Symbol jdbc2RascalType(struct()) { throw unsupportedJDBCType(struct()); }
public Symbol jdbc2RascalType(time()) = \datetime();
public Symbol jdbc2RascalType(timeStamp()) = \datetime();
public Symbol jdbc2RascalType(tinyInt()) = \int();
public Symbol jdbc2RascalType(varBinary()) = \list(\int());
public Symbol jdbc2RascalType(varChar()) = \str();

@doc{Represents values which may or may not be null.}
data Nullable[&T] = null() | notnull(&T item);

@doc{Load the contents of a table.}
@javaClass{org.rascalmpl.library.experiments.resource.JDBC}
public java list[&T] loadTable(type[&T] resType, Connection connection, str tableName);

@resource{jdbctable}
public str tableSchema(str moduleName, loc uri) {
	if (uri.scheme != "jdbctable") throw unexpectedScheme("jdbctable",head(parts),uri);

	// This indicates which driver we need (MySQL, Oracle, etc)
	driverType = uri.host;
	if (driverType notin drivers) throw "Driver not found";

	// The URI should be of the form host/database/table, get these pieces out
	tableName = uri.file;
	dbName = uri.parent.file;
	hostName = uri.parent.parent.file;	
	
	// These are the rest of the params, which should include everything
	// needed to connect plus some params that alter the generation
	list[tuple[str,str]] params = [ ];
	for (qp <- split("&", uri.query)) {
		ops = split("=",qp);
		if (size(ops) == 2)
			params += < uriDecode(ops[0]), uriDecode(ops[1]) >;
		else if (size(ops) > 2)
			throw "Unexpected option"; // todo: replace with an exception constructor
	}
	println("Computed parameters: <params>");
	
	// We can pass the name of the function to generate. If we did, grab it then remove
	// it from the params, which should just contain those needed by the JDBC driver.
	str funname = "resourceValue";
	for ( < "funname", fn > <- params ) funname = fn;
	params = [ <x,y> | <x,y> <- params, x != "funname" ];
	
	// Now, create the connect string, based on the type of database and the
	// parameters passed in.
	params = [<"host",hostName>,<"database",dbName>] + params;
	connectString = connectStringGenerators[driverType](params);
	
	// We need to connect to get the type of the table, which is used below
	registerJDBCClass(drivers[driverType]);
	con = createConnection(connectString);
	t = getTable(con,tableName);
	
	columnTypes = [ nullable ? \label("\\<cn>",\adt("Nullable",[rt])) : \label("\\<cn>",rt) | table(tn,cl) := t, column(cn,ct,nullable) <- cl, rt := jdbc2RascalType(ct) ];
	columnTuple = \tuple(columnTypes);
	
	closeConnection(con);
	
	mbody = "module <moduleName>
			'import JDBC;
			'public list[<prettyPrintType(columnTuple)>] <funname>() {
			'	registerJDBCClass(\"<drivers[driverType]>\");
			'	con = createConnection(\"<connectString>\");
			'	list[<prettyPrintType(columnTuple)>] res = loadTable(#<prettyPrintType(columnTuple)>,con,\"<tableName>\");
			'	closeConnection(con);
			'	return res;
			'}
			'";
	
	return mbody;
}