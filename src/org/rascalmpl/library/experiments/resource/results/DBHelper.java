package org.rascalmpl.library.experiments.resource.results;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;

import org.eclipse.imp.pdb.facts.IListWriter;
import org.eclipse.imp.pdb.facts.IValue;
import org.eclipse.imp.pdb.facts.type.Type;
import org.eclipse.imp.pdb.facts.type.TypeFactory;
import org.eclipse.imp.pdb.facts.type.TypeStore;
import org.rascalmpl.interpreter.staticErrors.UnsupportedOperationError;
import org.rascalmpl.values.ValueFactoryFactory;

public class DBHelper {

	static Type jdbc2pdbType(int columnType, TypeStore ts) {
		return DBHelper.jdbc2pdbType(columnType, false, ts);
	}

	public static Type jdbc2pdbType(int columnType, boolean nullable, TypeStore ts) {
		Type res = null;

		switch(columnType) {
			case Types.ARRAY:
				throw new UnsupportedOperationError("JDBC Array types are currently not supported", null);
			case Types.BIGINT:
				res = TypeFactory.getInstance().integerType();
				break;
			case Types.BINARY:
				res = TypeFactory.getInstance().listType(TypeFactory.getInstance().integerType());
				break;
			case Types.BIT:
				res = TypeFactory.getInstance().boolType();
				break;
			case Types.BLOB:
				res = TypeFactory.getInstance().listType(TypeFactory.getInstance().integerType());
				break;
			case Types.BOOLEAN:
				res = TypeFactory.getInstance().boolType();
				break;
			case Types.CHAR:
				res = TypeFactory.getInstance().stringType();
				break;
			case Types.CLOB:
				res = TypeFactory.getInstance().stringType();
				break;
			case Types.DATALINK:
				throw new UnsupportedOperationError("JDBC Datalink types are currently not supported", null);
			case Types.DATE:
				res = TypeFactory.getInstance().dateTimeType();
				break;
			case Types.DECIMAL:
				res = TypeFactory.getInstance().realType();
				break;
			case Types.DISTINCT:
				throw new UnsupportedOperationError("JDBC Distinct types are currently not supported", null);
			case Types.DOUBLE:
				res = TypeFactory.getInstance().realType();
				break;
			case Types.FLOAT:
				res = TypeFactory.getInstance().realType();
				break;
			case Types.INTEGER:
				res = TypeFactory.getInstance().integerType();
				break;
			case Types.JAVA_OBJECT:
				throw new UnsupportedOperationError("JDBC JavaObject types are currently not supported", null);
			case Types.LONGNVARCHAR:
				res = TypeFactory.getInstance().stringType();
				break;
			case Types.LONGVARBINARY:
				res = TypeFactory.getInstance().listType(TypeFactory.getInstance().integerType());
				break;
			case Types.LONGVARCHAR:
				res = TypeFactory.getInstance().stringType();
				break;
			case Types.NCHAR:
				res = TypeFactory.getInstance().stringType();
				break;
			case Types.NCLOB:
				res = TypeFactory.getInstance().stringType();
				break;
			case Types.NULL:
				throw new UnsupportedOperationError("JDBC Null types are currently not supported", null);
			case Types.NUMERIC:
				res = TypeFactory.getInstance().realType();
				break;
			case Types.NVARCHAR:
				res = TypeFactory.getInstance().stringType();
				break;
			case Types.OTHER:
				throw new UnsupportedOperationError("JDBC Other types are currently not supported", null);
			case Types.REAL:
				res = TypeFactory.getInstance().realType();
				break;
			case Types.REF:
				throw new UnsupportedOperationError("JDBC Ref types are currently not supported", null);
			case Types.ROWID:
				throw new UnsupportedOperationError("JDBC RowID types are currently not supported", null);
			case Types.SMALLINT:
				res = TypeFactory.getInstance().integerType();
				break;
			case Types.SQLXML:
				throw new UnsupportedOperationError("JDBC SQLXML types are currently not supported", null);
			case Types.STRUCT:
				throw new UnsupportedOperationError("JDBC Struct types are currently not supported", null);
			case Types.TIME:
				res = TypeFactory.getInstance().dateTimeType();
				break;
			case Types.TIMESTAMP:
				res = TypeFactory.getInstance().dateTimeType();
				break;
			case Types.TINYINT:
				res = TypeFactory.getInstance().integerType();
				break;
			case Types.VARBINARY:
				res = TypeFactory.getInstance().listType(TypeFactory.getInstance().integerType());
				break;
			case Types.VARCHAR:
				res = TypeFactory.getInstance().stringType();
				break;
		}
		
		if (nullable) {
//			TypeStore ts = new TypeStore();
			Type paramType = TypeFactory.getInstance().parameterType("T");
			Type nullADT = TypeFactory.getInstance().abstractDataType(ts, "Nullable", paramType);
			HashMap<Type,Type> bindings = new HashMap<Type,Type>();

			bindings.put(paramType, res);
			res = nullADT.instantiate(bindings);
		}
		
		return res;
	}
	
	public static IValue jdbc2pdbValue(ResultSet rs, int idx, Type columnType, TypeStore ts) {
		IValue res = null;
		
		try {
			int jdbcColumnType = rs.getMetaData().getColumnType(idx);
			Calendar c = Calendar.getInstance();
			IListWriter lw = null;
			InputStream isr = null;
			int isrRes = -1;
			
			// TODO: Need to come up with a good representation for nullable values, right now
			// I'm just picking a default which makes sense, but this isn't right (null is not 0!)
			switch(jdbcColumnType) {
				case Types.ARRAY:
					throw new UnsupportedOperationError("JDBC Array types are currently not supported", null);
				case Types.BIGINT:
					if (rs.getBigDecimal(idx) != null)
						res = ValueFactoryFactory.getValueFactory().integer(rs.getBigDecimal(idx).toString());
					else
						res = ValueFactoryFactory.getValueFactory().integer(0);
					break;
				case Types.BINARY:
					isr = rs.getBinaryStream(idx);
					lw = ValueFactoryFactory.getValueFactory().listWriter(TypeFactory.getInstance().integerType());
					if (isr != null) {
						isrRes = isr.read();
						while (isrRes != -1) {
							lw.append(ValueFactoryFactory.getValueFactory().integer(isrRes));
							isrRes = isr.read();
						}
					}
					res = lw.done();
					break;
				case Types.BIT:
					res = ValueFactoryFactory.getValueFactory().bool(rs.getBoolean(idx));
					break;
				case Types.BLOB:
					lw = ValueFactoryFactory.getValueFactory().listWriter(TypeFactory.getInstance().integerType());
					if (rs.getBlob(idx) != null) {
						isr = rs.getBlob(idx).getBinaryStream();
						if (isr != null) {
							isrRes = isr.read();
							while (isrRes != -1) {
								lw.append(ValueFactoryFactory.getValueFactory().integer(isrRes));
								isrRes = isr.read();
							}
						}
					}
					res = lw.done();
					break;
				case Types.BOOLEAN:
					res = ValueFactoryFactory.getValueFactory().bool(rs.getBoolean(idx));
					break;
				case Types.CHAR:
					if (rs.getString(idx) != null)
						res = ValueFactoryFactory.getValueFactory().string(rs.getString(idx));
					else
						res = ValueFactoryFactory.getValueFactory().string("");
					break;
				case Types.CLOB:
					lw = ValueFactoryFactory.getValueFactory().listWriter(TypeFactory.getInstance().integerType());
					if (rs.getClob(idx) != null) {
						isr = rs.getClob(idx).getAsciiStream();
						if (isr != null) {
							isrRes = isr.read();
							while (isrRes != -1) {
								lw.append(ValueFactoryFactory.getValueFactory().integer(isrRes));
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
					res = ValueFactoryFactory.getValueFactory().date(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH));
					break;
				case Types.DECIMAL:
					if (rs.getBigDecimal(idx) != null)
						res = ValueFactoryFactory.getValueFactory().real(rs.getBigDecimal(idx).toString());
					else
						res = ValueFactoryFactory.getValueFactory().real(0.0);
					break;
				case Types.DISTINCT:
					throw new UnsupportedOperationError("JDBC Distinct types are currently not supported", null);
				case Types.DOUBLE:
					res = ValueFactoryFactory.getValueFactory().real(rs.getDouble(idx));
					break;
				case Types.FLOAT:
					res = ValueFactoryFactory.getValueFactory().real(rs.getFloat(idx));
					break;
				case Types.INTEGER:
					res = ValueFactoryFactory.getValueFactory().integer(rs.getInt(idx));
					break;
				case Types.JAVA_OBJECT:
					throw new UnsupportedOperationError("JDBC JavaObject types are currently not supported", null);
				case Types.LONGNVARCHAR:
					if (rs.getString(idx) != null)
						res = ValueFactoryFactory.getValueFactory().string(rs.getString(idx));
					else
						res = ValueFactoryFactory.getValueFactory().string("");
					break;
				case Types.LONGVARBINARY:
					lw = ValueFactoryFactory.getValueFactory().listWriter(TypeFactory.getInstance().integerType());
					isr = rs.getBinaryStream(idx);
					if (isr != null) {
						isrRes = isr.read();
						while (isrRes != -1) {
							lw.append(ValueFactoryFactory.getValueFactory().integer(isrRes));
							isrRes = isr.read();
						}
					}
					res = lw.done();
					break;
				case Types.LONGVARCHAR:
					if (rs.getString(idx) != null)
						res = ValueFactoryFactory.getValueFactory().string(rs.getString(idx));
					else
						res = ValueFactoryFactory.getValueFactory().string("");
					break;
				case Types.NCHAR:
					if (rs.getString(idx) != null)
						res = ValueFactoryFactory.getValueFactory().string(rs.getString(idx));
					else
						res = ValueFactoryFactory.getValueFactory().string("");
					break;
				case Types.NCLOB:
					lw = ValueFactoryFactory.getValueFactory().listWriter(TypeFactory.getInstance().integerType());
					if (rs.getNClob(idx) != null) {
						isr = rs.getNClob(idx).getAsciiStream();
						if (isr != null) {
							isrRes = isr.read();
							while (isrRes != -1) {
								lw.append(ValueFactoryFactory.getValueFactory().integer(isrRes));
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
						res = ValueFactoryFactory.getValueFactory().real(rs.getBigDecimal(idx).toString());
					} else {
						res = ValueFactoryFactory.getValueFactory().real(0);
					}
					break;
				case Types.NVARCHAR:
					if (rs.getString(idx) != null)
						res = ValueFactoryFactory.getValueFactory().string(rs.getString(idx));
					else
						res = ValueFactoryFactory.getValueFactory().string("");
					break;
				case Types.OTHER:
					throw new UnsupportedOperationError("JDBC Other types are currently not supported", null);
				case Types.REAL:
					res = ValueFactoryFactory.getValueFactory().real(rs.getDouble(idx));
					break;
				case Types.REF:
					throw new UnsupportedOperationError("JDBC Ref types are currently not supported", null);
				case Types.ROWID:
					throw new UnsupportedOperationError("JDBC RowID types are currently not supported", null);
				case Types.SMALLINT:
					res = ValueFactoryFactory.getValueFactory().integer(rs.getInt(idx));
					break;
				case Types.SQLXML:
					throw new UnsupportedOperationError("JDBC SQLXML types are currently not supported", null);
				case Types.STRUCT:
					throw new UnsupportedOperationError("JDBC Struct types are currently not supported", null);
				case Types.TIME:
					if (rs.getTime(idx) != null)
						c.setTime(rs.getTime(idx));
					res = ValueFactoryFactory.getValueFactory().time(c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), c.get(Calendar.MILLISECOND));
					break;
				case Types.TIMESTAMP:
					if (rs.getTimestamp(idx) != null)
						c.setTime(rs.getTimestamp(idx));
					res = ValueFactoryFactory.getValueFactory().datetime(c.get(Calendar.YEAR), c.get(Calendar.MONTH), c.get(Calendar.DAY_OF_MONTH), c.get(Calendar.HOUR_OF_DAY), c.get(Calendar.MINUTE), c.get(Calendar.SECOND), c.get(Calendar.MILLISECOND));
					break;
				case Types.TINYINT:
					res = ValueFactoryFactory.getValueFactory().integer(rs.getInt(idx));
					break;
				case Types.VARBINARY:
					lw = ValueFactoryFactory.getValueFactory().listWriter(TypeFactory.getInstance().integerType());
					isr = rs.getBinaryStream(idx);
					if (isr != null) {
						isrRes = isr.read();
						while (isrRes != -1) {
							lw.append(ValueFactoryFactory.getValueFactory().integer(isrRes));
							isrRes = isr.read();
						}
					}
					res = lw.done();
					break;
				case Types.VARCHAR:
					if (rs.getString(idx) != null)
						res = ValueFactoryFactory.getValueFactory().string(rs.getString(idx));
					else
						res = ValueFactoryFactory.getValueFactory().string("");
					break;
			}

			if (columnType.isAbstractDataType() && columnType.getName().equals("Nullable")) {
//				TypeStore ts = new TypeStore();
				Type paramType = TypeFactory.getInstance().parameterType("T");
				Type nullADT = TypeFactory.getInstance().abstractDataType(ts, "Nullable", paramType);
				Type notnullC = TypeFactory.getInstance().constructor(ts, nullADT, "notnull", paramType, "v");
				Type nullC = TypeFactory.getInstance().constructor(ts, nullADT, "null");
				HashMap<Type,Type> bindings = new HashMap<Type,Type>();

				Type resType = jdbc2pdbType(jdbcColumnType, true, ts);
				bindings.put(paramType, resType);
				nullADT = nullADT.instantiate(bindings);


				if (rs.wasNull()) {
					res = ValueFactoryFactory.getValueFactory().constructor(nullC);
				} else {
					res = ValueFactoryFactory.getValueFactory().constructor(notnullC, res);
				}
			}

		} catch (SQLException sqle) {
			// TODO: Throw here...
		} catch (IOException ioe) {
			// TODO: Throw here
		}
		

		return res;
	}
	
}
