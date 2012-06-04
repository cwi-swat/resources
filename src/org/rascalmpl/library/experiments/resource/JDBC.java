package org.rascalmpl.library.experiments.resource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;

import org.eclipse.imp.pdb.facts.IConstructor;
import org.eclipse.imp.pdb.facts.IInteger;
import org.eclipse.imp.pdb.facts.IList;
import org.eclipse.imp.pdb.facts.IListWriter;
import org.eclipse.imp.pdb.facts.ISet;
import org.eclipse.imp.pdb.facts.ISetWriter;
import org.eclipse.imp.pdb.facts.IString;
import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.IValueFactory;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeFactory;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.rascalmpl.interpreter.staticErrors.UnsupportedOperationError;
import org.rascalmpl.interpreter.utils.RuntimeExceptionFactory;
import org.rascalmpl.values.ValueFactoryFactory;

public class JDBC {

	private static TypeFactory TF = TypeFactory.getInstance();
	public static final TypeStore TS = new TypeStore();
	
	public static final Type Connection = TF.abstractDataType(TS, "Connection");
	public static final Type jdbcConnection = TF.constructor(TS, Connection, "jdbcConnection", TF.integerType(), "id");

	public static final Type JDBCType = TF.abstractDataType(TS, "JDBCType");
	public static final Type jdbcArray = TF.constructor(TS, JDBCType, "array");
	public static final Type jdbcBigInt = TF.constructor(TS, JDBCType, "bigInt");
	public static final Type jdbcBinary = TF.constructor(TS, JDBCType, "binary");
	public static final Type jdbcBit = TF.constructor(TS, JDBCType, "bit");
	public static final Type jdbcBlob = TF.constructor(TS, JDBCType, "blob");
	public static final Type jdbcBoolean = TF.constructor(TS, JDBCType, "boolean");
	public static final Type jdbcChar = TF.constructor(TS, JDBCType, "char");
	public static final Type jdbcClob = TF.constructor(TS, JDBCType, "clob");
	public static final Type jdbcDataLink = TF.constructor(TS, JDBCType, "dataLink");
	public static final Type jdbcDate = TF.constructor(TS, JDBCType, "date");
	public static final Type jdbcDecimal = TF.constructor(TS, JDBCType, "decimal");
	public static final Type jdbcDistinct = TF.constructor(TS, JDBCType, "distinct");
	public static final Type jdbcDouble = TF.constructor(TS, JDBCType, "double");
	public static final Type jdbcFloat = TF.constructor(TS, JDBCType, "float");
	public static final Type jdbcInteger = TF.constructor(TS, JDBCType, "integer");
	public static final Type jdbcJavaObject = TF.constructor(TS, JDBCType, "javaObject");
	public static final Type jdbcLongNVarChar = TF.constructor(TS, JDBCType, "longNVarChar");
	public static final Type jdbcLongVarBinary = TF.constructor(TS, JDBCType, "longVarBinary");
	public static final Type jdbcLongVarChar = TF.constructor(TS, JDBCType, "longVarChar");
	public static final Type jdbcNChar = TF.constructor(TS, JDBCType, "nChar");
	public static final Type jdbcNClob = TF.constructor(TS, JDBCType, "nClob");
	public static final Type jdbcNull = TF.constructor(TS, JDBCType, "null");
	public static final Type jdbcNumeric = TF.constructor(TS, JDBCType, "numeric");
	public static final Type jdbcNVarChar = TF.constructor(TS, JDBCType, "nVarChar");
	public static final Type jdbcOther = TF.constructor(TS, JDBCType, "other");
	public static final Type jdbcReal = TF.constructor(TS, JDBCType, "real");
	public static final Type jdbcRef = TF.constructor(TS, JDBCType, "ref");
	public static final Type jdbcRowId = TF.constructor(TS, JDBCType, "rowId");
	public static final Type jdbcSmallInt = TF.constructor(TS, JDBCType, "smallInt");
	public static final Type jdbcSQLXML = TF.constructor(TS, JDBCType, "sqlXML");
	public static final Type jdbcStruct = TF.constructor(TS, JDBCType, "struct");
	public static final Type jdbcTime = TF.constructor(TS, JDBCType, "time");
	public static final Type jdbcTimeStamp = TF.constructor(TS, JDBCType, "timeStamp");
	public static final Type jdbcTinyInt = TF.constructor(TS, JDBCType, "tinyInt");
	public static final Type jdbcVarBinary = TF.constructor(TS, JDBCType, "varBinary");
	public static final Type jdbcVarChar = TF.constructor(TS, JDBCType, "varChar");

	public static final Type Column = TF.abstractDataType(TS, "Column");
	public static final Type column = TF.constructor(TS, Column, "column", TF.stringType(), "columnName", JDBCType, "columnType", TF.boolType(), "nullable");
	
	public static final Type Table = TF.abstractDataType(TS, "Table");
	public static final Type table = TF.constructor(TS, Table, "table", TF.stringType(), "tableName", TF.listType(Column), "columns");

	public static final Type nullableT = TF.parameterType("T");
	public static final Type Nullable = TF.abstractDataType(TS, "Nullable", nullableT);

	private final IValueFactory vf;
	private int connectionCounter = 0;
	private HashMap<IInteger,Connection> connectionMap;

	public JDBC(IValueFactory vf) {
		this.vf = vf;
		this.connectionMap = new HashMap<IInteger,Connection>();
	}
	
	public void registerJDBCClass(IString className) {
		try {
			Class.forName(className.getValue());
		} catch (ClassNotFoundException cnfe) {
			throw RuntimeExceptionFactory.illegalArgument(className, null, cnfe.getMessage(), "Could not load driver");
		}
	}
	
	public IConstructor createConnection(IString connectString) {
		try {
			Connection conn =  DriverManager.getConnection(connectString.getValue());
			IInteger newKey = vf.integer(++connectionCounter);
			connectionMap.put(newKey, conn);
			return vf.constructor(JDBC.jdbcConnection, newKey);
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connectString, null, sqle.getMessage(), "Could not connect with given connect string");
		}
	}
	
	public void closeConnection(IConstructor connection) {
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				conn.close();
				connectionMap.remove(connectionId);
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage(), "Could not close the given connection");
		}
	}

	public IList getTableTypes(IConstructor connection) {
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				DatabaseMetaData dmd = conn.getMetaData();
				ResultSet rs = dmd.getTableTypes();
				IListWriter resultWriter = this.vf.listWriter(TF.stringType());
				while (rs.next()) resultWriter.append(this.vf.string(rs.getString(1)));
				rs.close();
				return resultWriter.done();
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage(), "Could not close the given connection");
		}
	}

	public ISet getTableNames(IConstructor connection) {
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				DatabaseMetaData dmd = conn.getMetaData();
				ResultSet rs = dmd.getTables(null, null, null, new String[] { "TABLE" });
				HashSet<String> tables = new HashSet<String>();
				while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
				rs.close();
				
				ISetWriter setRes = vf.setWriter(TF.stringType());
				
				for (String tableName : tables) {
					setRes.insert(vf.string(tableName));
				}
				return setRes.done();
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage(), "Could not close the given connection");
		}
	}

	public ISet getViewNames(IConstructor connection) {
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				DatabaseMetaData dmd = conn.getMetaData();
				ResultSet rs = dmd.getTables(null, null, null, new String[] { "VIEW" });
				HashSet<String> tables = new HashSet<String>();
				while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
				rs.close();
				
				ISetWriter setRes = vf.setWriter(TF.stringType());
				
				for (String tableName : tables) {
					setRes.insert(vf.string(tableName));
				}
				return setRes.done();
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage(), "Could not close the given connection");
		}
	}

	public ISet getTables(IConstructor connection) {
		return getTablesOrViews(connection, new String[] { "TABLE" });
	}
	
	public ISet getViews(IConstructor connection) {
		return getTablesOrViews(connection, new String[] { "VIEW" });
	}
	
	private ISet getTablesOrViews(IConstructor connection, String[] tableTypes) {
		// TODO: Add code to check and make sure the table types are valid
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				DatabaseMetaData dmd = conn.getMetaData();
				ResultSet rs = dmd.getTables(null, null, null, tableTypes);
				HashSet<String> tables = new HashSet<String>();
				while (rs.next()) tables.add(rs.getString("TABLE_NAME"));
				rs.close();
				
				ISetWriter setRes = vf.setWriter(Table);
				
				for (String tableName : tables) {
					rs = dmd.getColumns(null, null, tableName, null);
					IListWriter listRes = vf.listWriter(Column); 
					while (rs.next()) {
						String cn = rs.getString("COLUMN_NAME");
						int dt = rs.getInt("DATA_TYPE");
						String nullable = rs.getString("IS_NULLABLE");
						listRes.append(vf.constructor(column, vf.string(cn), vf.constructor(JDBC.jdbc2rascalType(dt)), nullable.equalsIgnoreCase("YES") ? vf.bool(true) : vf.bool(false)));
					}
					setRes.insert(vf.constructor(table, vf.string(tableName), listRes.done()));
					rs.close();
				}
				return setRes.done();
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage(), "Could not close the given connection");
		}		
	}
	
	// TODO: Handle the case where the table name does not exist
	public IConstructor getTable(IConstructor connection, IString tableName) {
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				DatabaseMetaData dmd = conn.getMetaData();
				ResultSet rs = dmd.getColumns(null, null, tableName.getValue(), null);
				IListWriter listRes = vf.listWriter(Column); 
				while (rs.next()) {
					String cn = rs.getString("COLUMN_NAME");
					int dt = rs.getInt("DATA_TYPE");
					String nullable = rs.getString("IS_NULLABLE");
					listRes.append(vf.constructor(column, vf.string(cn), vf.constructor(JDBC.jdbc2rascalType(dt)), nullable.equalsIgnoreCase("YES") ? vf.bool(true) : vf.bool(false)));
				}
				rs.close();
				return vf.constructor(table, tableName, listRes.done());
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage(), "Could not close the given connection");
		}
	}

	private static Type jdbc2rascalType(int columnType) {
		switch(columnType) {
			case Types.ARRAY:
				return JDBC.jdbcArray;
			case Types.BIGINT:
				return JDBC.jdbcBigInt;
			case Types.BINARY:
				return JDBC.jdbcBinary;
			case Types.BIT:
				return JDBC.jdbcBit;
			case Types.BLOB:
				return JDBC.jdbcBlob;
			case Types.BOOLEAN:
				return JDBC.jdbcBoolean;
			case Types.CHAR:
				return JDBC.jdbcChar;
			case Types.CLOB:
				return JDBC.jdbcClob;
			case Types.DATALINK:
				return JDBC.jdbcDataLink;
			case Types.DATE:
				return JDBC.jdbcDate;
			case Types.DECIMAL:
				return JDBC.jdbcDecimal;
			case Types.DISTINCT:
				return JDBC.jdbcDistinct;
			case Types.DOUBLE:
				return JDBC.jdbcDouble;
			case Types.FLOAT:
				return JDBC.jdbcFloat;
			case Types.INTEGER:
				return JDBC.jdbcInteger;
			case Types.JAVA_OBJECT:
				return JDBC.jdbcJavaObject;
			case Types.LONGNVARCHAR:
				return JDBC.jdbcLongNVarChar;
			case Types.LONGVARBINARY:
				return JDBC.jdbcLongVarBinary;
			case Types.LONGVARCHAR:
				return JDBC.jdbcLongVarChar;
			case Types.NCHAR:
				return JDBC.jdbcNChar;
			case Types.NCLOB:
				return JDBC.jdbcNClob;
			case Types.NULL:
				return JDBC.jdbcNull;
			case Types.NUMERIC:
				return JDBC.jdbcNumeric;
			case Types.NVARCHAR:
				return JDBC.jdbcNVarChar;
			case Types.OTHER:
				return JDBC.jdbcOther;
			case Types.REAL:
				return JDBC.jdbcReal;
			case Types.REF:
				return JDBC.jdbcRef;
			case Types.ROWID:
				return JDBC.jdbcRowId;
			case Types.SMALLINT:
				return JDBC.jdbcSmallInt;
			case Types.SQLXML:
				return JDBC.jdbcSQLXML;
			case Types.STRUCT:
				return JDBC.jdbcStruct;
			case Types.TIME:
				return JDBC.jdbcTime;
			case Types.TIMESTAMP:
				return JDBC.jdbcTimeStamp;
			case Types.TINYINT:
				return JDBC.jdbcTinyInt;
			case Types.VARBINARY:
				return JDBC.jdbcVarBinary;
			case Types.VARCHAR:
				return JDBC.jdbcVarChar;
		}
		throw RuntimeExceptionFactory.illegalArgument(ValueFactoryFactory.getValueFactory().integer(columnType), null, null, "Invalid JDBC type id given: " + columnType);
	}

	public static Type jdbc2pdbType(int columnType, boolean nullable) {
		Type res = null;

		switch(columnType) {
			case Types.ARRAY:
				throw new UnsupportedOperationError("JDBC Array types are currently not supported", null);
			case Types.BIGINT:
				res = TF.integerType();
				break;
			case Types.BINARY:
				res = TF.listType(TF.integerType());
				break;
			case Types.BIT:
				res = TF.boolType();
				break;
			case Types.BLOB:
				res = TF.listType(TF.integerType());
				break;
			case Types.BOOLEAN:
				res = TF.boolType();
				break;
			case Types.CHAR:
				res = TF.stringType();
				break;
			case Types.CLOB:
				res = TF.stringType();
				break;
			case Types.DATALINK:
				throw new UnsupportedOperationError("JDBC Datalink types are currently not supported", null);
			case Types.DATE:
				res = TF.dateTimeType();
				break;
			case Types.DECIMAL:
				res = TF.realType();
				break;
			case Types.DISTINCT:
				throw new UnsupportedOperationError("JDBC Distinct types are currently not supported", null);
			case Types.DOUBLE:
				res = TF.realType();
				break;
			case Types.FLOAT:
				res = TF.realType();
				break;
			case Types.INTEGER:
				res = TF.integerType();
				break;
			case Types.JAVA_OBJECT:
				throw new UnsupportedOperationError("JDBC JavaObject types are currently not supported", null);
			case Types.LONGNVARCHAR:
				res = TF.stringType();
				break;
			case Types.LONGVARBINARY:
				res = TF.listType(TF.integerType());
				break;
			case Types.LONGVARCHAR:
				res = TF.stringType();
				break;
			case Types.NCHAR:
				res = TF.stringType();
				break;
			case Types.NCLOB:
				res = TF.stringType();
				break;
			case Types.NULL:
				throw new UnsupportedOperationError("JDBC Null types are currently not supported", null);
			case Types.NUMERIC:
				res = TF.realType();
				break;
			case Types.NVARCHAR:
				res = TF.stringType();
				break;
			case Types.OTHER:
				throw new UnsupportedOperationError("JDBC Other types are currently not supported", null);
			case Types.REAL:
				res = TF.realType();
				break;
			case Types.REF:
				throw new UnsupportedOperationError("JDBC Ref types are currently not supported", null);
			case Types.ROWID:
				throw new UnsupportedOperationError("JDBC RowID types are currently not supported", null);
			case Types.SMALLINT:
				res = TF.integerType();
				break;
			case Types.SQLXML:
				throw new UnsupportedOperationError("JDBC SQLXML types are currently not supported", null);
			case Types.STRUCT:
				throw new UnsupportedOperationError("JDBC Struct types are currently not supported", null);
			case Types.TIME:
				res = TF.dateTimeType();
				break;
			case Types.TIMESTAMP:
				res = TF.dateTimeType();
				break;
			case Types.TINYINT:
				res = TF.integerType();
				break;
			case Types.VARBINARY:
				res = TF.listType(TF.integerType());
				break;
			case Types.VARCHAR:
				res = TF.stringType();
				break;
		}
		
		if (nullable) {
			HashMap<Type,Type> bindings = new HashMap<Type,Type>();
			bindings.put(nullableT, res);
			res = Nullable.instantiate(bindings);
		}
		
		return res;
	}
	
	public static IValue jdbc2pdbValue(ResultSet rs, int idx, Type columnType, IValueFactory vf) {
		IValue res = null;
		
		try {
			int jdbcColumnType = rs.getMetaData().getColumnType(idx);
			Calendar c = Calendar.getInstance();
			IListWriter lw = null;
			InputStream isr = null;
			int isrRes = -1;
			
			switch(jdbcColumnType) {
				case Types.ARRAY:
					throw new UnsupportedOperationError("JDBC Array types are currently not supported", null);
				case Types.BIGINT:
					if (rs.getBigDecimal(idx) != null)
						res = vf.integer(rs.getBigDecimal(idx).toString());
					else
						res = vf.integer(0);
					break;
				case Types.BINARY:
					isr = rs.getBinaryStream(idx);
					lw = vf.listWriter(TypeFactory.getInstance().integerType());
					if (isr != null) {
						isrRes = isr.read();
						while (isrRes != -1) {
							lw.append(vf.integer(isrRes));
							isrRes = isr.read();
						}
					}
					res = lw.done();
					break;
				case Types.BIT:
					res = vf.bool(rs.getBoolean(idx));
					break;
				case Types.BLOB:
					lw = vf.listWriter(TypeFactory.getInstance().integerType());
					if (rs.getBlob(idx) != null) {
						isr = rs.getBlob(idx).getBinaryStream();
						if (isr != null) {
							isrRes = isr.read();
							while (isrRes != -1) {
								lw.append(vf.integer(isrRes));
								isrRes = isr.read();
							}
						}
					}
					res = lw.done();
					break;
				case Types.BOOLEAN:
					res = vf.bool(rs.getBoolean(idx));
					break;
				case Types.CHAR:
					if (rs.getString(idx) != null)
						res = vf.string(rs.getString(idx));
					else
						res = vf.string("");
					break;
				case Types.CLOB:
					lw = vf.listWriter(TypeFactory.getInstance().integerType());
					if (rs.getClob(idx) != null) {
						isr = rs.getClob(idx).getAsciiStream();
						if (isr != null) {
							isrRes = isr.read();
							while (isrRes != -1) {
								lw.append(vf.integer(isrRes));
								isrRes = isr.read();
							}
						}
					}
					res = lw.done();
					break;
				case Types.DATALINK:
					throw new UnsupportedOperationError("JDBC Datalink types are currently not supported", null);
				case Types.DATE:
					if (rs.getDate(idx) != null) {
						c.setTime(rs.getDate(idx));
					}
					res = vf.date(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));
					break;
				case Types.DECIMAL:
					if (rs.getBigDecimal(idx) != null)
						res = vf.real(rs.getBigDecimal(idx).toString());
					else
						res = vf.real(0.0);
					break;
				case Types.DISTINCT:
					throw new UnsupportedOperationError("JDBC Distinct types are currently not supported", null);
				case Types.DOUBLE:
					res = vf.real(rs.getDouble(idx));
					break;
				case Types.FLOAT:
					res = vf.real(rs.getFloat(idx));
					break;
				case Types.INTEGER:
					res = vf.integer(rs.getInt(idx));
					break;
				case Types.JAVA_OBJECT:
					throw new UnsupportedOperationError("JDBC JavaObject types are currently not supported", null);
				case Types.LONGNVARCHAR:
					if (rs.getString(idx) != null)
						res = vf.string(rs.getString(idx));
					else
						res = vf.string("");
					break;
				case Types.LONGVARBINARY:
					lw = vf.listWriter(TypeFactory.getInstance().integerType());
					isr = rs.getBinaryStream(idx);
					if (isr != null) {
						isrRes = isr.read();
						while (isrRes != -1) {
							lw.append(vf.integer(isrRes));
							isrRes = isr.read();
						}
					}
					res = lw.done();
					break;
				case Types.LONGVARCHAR:
					if (rs.getString(idx) != null)
						res = vf.string(rs.getString(idx));
					else
						res = vf.string("");
					break;
				case Types.NCHAR:
					if (rs.getString(idx) != null)
						res = vf.string(rs.getString(idx));
					else
						res = vf.string("");
					break;
				case Types.NCLOB:
					lw = vf.listWriter(TypeFactory.getInstance().integerType());
					if (rs.getNClob(idx) != null) {
						isr = rs.getNClob(idx).getAsciiStream();
						if (isr != null) {
							isrRes = isr.read();
							while (isrRes != -1) {
								lw.append(vf.integer(isrRes));
								isrRes = isr.read();
							}
						}
					}
					res = lw.done();
					break;
				case Types.NULL:
					throw new UnsupportedOperationError("JDBC Null types are currently not supported", null);
				case Types.NUMERIC:
					if (rs.getBigDecimal(idx) != null) {
						res = vf.real(rs.getBigDecimal(idx).toString());
					} else {
						res = vf.real(0);
					}
					break;
				case Types.NVARCHAR:
					if (rs.getString(idx) != null)
						res = vf.string(rs.getString(idx));
					else
						res = vf.string("");
					break;
				case Types.OTHER:
					throw new UnsupportedOperationError("JDBC Other types are currently not supported", null);
				case Types.REAL:
					res = vf.real(rs.getDouble(idx));
					break;
				case Types.REF:
					throw new UnsupportedOperationError("JDBC Ref types are currently not supported", null);
				case Types.ROWID:
					throw new UnsupportedOperationError("JDBC RowID types are currently not supported", null);
				case Types.SMALLINT:
					res = vf.integer(rs.getInt(idx));
					break;
				case Types.SQLXML:
					throw new UnsupportedOperationError("JDBC SQLXML types are currently not supported", null);
				case Types.STRUCT:
					throw new UnsupportedOperationError("JDBC Struct types are currently not supported", null);
				case Types.TIME:
					if (rs.getTime(idx) != null)
						c.setTime(rs.getTime(idx));
					res = vf.time(c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), c.get(Calendar.MILLISECOND));
					break;
				case Types.TIMESTAMP:
					if (rs.getTimestamp(idx) != null)
						c.setTime(rs.getTimestamp(idx));
					res = vf.datetime(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), c.get(Calendar.MILLISECOND));
					break;
				case Types.TINYINT:
					res = vf.integer(rs.getInt(idx));
					break;
				case Types.VARBINARY:
					lw = vf.listWriter(TypeFactory.getInstance().integerType());
					isr = rs.getBinaryStream(idx);
					if (isr != null) {
						isrRes = isr.read();
						while (isrRes != -1) {
							lw.append(vf.integer(isrRes));
							isrRes = isr.read();
						}
					}
					res = lw.done();
					break;
				case Types.VARCHAR:
					if (rs.getString(idx) != null)
						res = vf.string(rs.getString(idx));
					else
						res = vf.string("");
					break;
			}

			if (columnType.isAbstractDataType() && columnType.getName().equals("Nullable")) {
				HashMap<Type,Type> bindings = new HashMap<Type,Type>();
				Type resType = jdbc2pdbType(jdbcColumnType, true);
				bindings.put(nullableT, resType);
				Type wrapperType = Nullable.instantiate(bindings);


				if (rs.wasNull()) {
					Type nullT = TF.constructor(TS,  wrapperType, "null");
					res = vf.constructor(nullT);
				} else {
					Type notnullT = TF.constructor(TS, wrapperType, "notnull", resType, "item");
					res = vf.constructor(notnullT, res);
				}
			}

		} catch (SQLException sqle) {
			// TODO: Throw here...
		} catch (IOException ioe) {
			// TODO: Throw here
		}
		

		return res;
	}	
	
	// TODO: Add more error handling code...
	public IValue loadTable(IValue resultType, IConstructor connection, IString tableName) {
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + tableName.getValue());
				ResultSet rs = stmt.executeQuery();
				
				Type elementType = resultType.getType().getTypeParameters().getFieldType(0);
				int columns = elementType.getArity();

				ISetWriter sw = vf.setWriter(elementType);
				while (rs.next()) {
					IValue tupleValues[] = new IValue[columns];
					for (int idx = 0; idx < columns; ++idx) {
						tupleValues[idx] = JDBC.jdbc2pdbValue(rs, idx + 1, elementType.getFieldType(idx), this.vf);
					}
					sw.insert(vf.tuple(tupleValues));
				}
				
				rs.close();
				stmt.close();
				
				return sw.done();
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage());
		}
	}

	public IValue loadTableOrdered(IValue resultType, IConstructor connection, IString tableName) {
		try {
			IInteger connectionId = (IInteger) connection.get(0);
			if (connectionMap.containsKey(connectionId)) {
				Connection conn = connectionMap.get(connectionId);
				PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + tableName.getValue());
				ResultSet rs = stmt.executeQuery();
				
				Type elementType = resultType.getType().getTypeParameters().getFieldType(0);
				int columns = elementType.getArity();

				IListWriter lw = vf.listWriter(elementType);
				while (rs.next()) {
					IValue tupleValues[] = new IValue[columns];
					for (int idx = 0; idx < columns; ++idx) {
						tupleValues[idx] = JDBC.jdbc2pdbValue(rs, idx + 1, elementType.getFieldType(idx), this.vf);
					}
					lw.append(vf.tuple(tupleValues));
				}
				
				rs.close();
				stmt.close();
				
				return lw.done();
			} else {
				throw RuntimeExceptionFactory.illegalArgument(connection, null, null, "Connection does not exist.");
			}
		} catch (SQLException sqle) {
			throw RuntimeExceptionFactory.illegalArgument(connection, null, sqle.getMessage());
		}
	}
}
