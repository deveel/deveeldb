using System;
using System.Collections;
using System.Data;

using Deveel.Shell;

namespace Deveel.Data.Shell {
	public sealed class SQLMetaDataBuilder {
		private readonly static String[] LIST_TABLES = { "TABLE" };
		private static readonly bool _verbose = false;

		// column description
		public const int TABLE_NAME = 2; // String
		public const int COLUMN_NAME = 3; // String
		public const int DATA_TYPE = 4; // int -> java.sql.Types
		public const int TYPE_NAME = 5; // String
		public const int COLUMN_SIZE = 6; // int
		public const int NULLABLE = 10; // int: 
		public const int COLUMN_DEF = 12; // String
		public const int ORDINAL_POSITION = 16; // int, starting at 1
		/*
		 * columnNoNulls - might not allow NULL values
		 * columnNullable - definitely allows NULL values
		 * columnNullableUnknown - nullability unknown 
		 */
		public const int IS_NULLABLE = 16;

		// primary key description
		public const int PK_DESC_COLUMN_NAME = 3;
		public const int PK_DESC_KEY_SEQ = 4;
		public const int PK_DESC_PK_NAME = 5;

		// foreign key description
		private const int FK_PKTABLE_NAME = 2;
		private const int FK_PKCOLUMN_NAME = 3;
		private const int FK_FKCOLUMN_NAME = 7;
		private const int FK_FK_NAME = 11;

		private bool _interrupted;

		/* (non-Javadoc)
		 * @see henplus.Interruptable#interrupt()
		 */
		public void interrupt() {
			_interrupted = true;
		}

		public SQLMetaData getMetaData(SQLSession session) {
			IDataReader rset = null;
			ArrayList tableList = new ArrayList();
			try {
				DatabaseMetaData meta = new DatabaseMetaData(session.Connection);
				rset = meta.getTables(null, null, null, LIST_TABLES);
				while (rset.Read()) {
					tableList.Add(rset.GetString(2));
				}
			} catch (Exception e) {
				// ignore.
			} finally {
				if (rset != null) {
					try { rset.Close(); } catch (Exception e) { }
				}
			}
			return getMetaData(session, tableList);
		}

		public SQLMetaData getMetaData(SQLSession session, ICollection /*<String>*/ tableNames) {
			return getMetaData(session, tableNames.GetEnumerator());
		}

		public SQLMetaData getMetaData(SQLSession session, IEnumerator /*<String>*/ tableNamesIter) {
			SQLMetaData result = new SQLMetaData();

			IDataReader rset = null;
			try {
				_interrupted = false;
				String catalog = session.Connection.Database;

				if (_interrupted)
					return null;

				DatabaseMetaData meta = new DatabaseMetaData(session.Connection);

				while (tableNamesIter.MoveNext() && !_interrupted) {
					String tableName = (String)tableNamesIter.Current;
					rset = meta.getColumns(catalog, null, tableName, null);
					Table table = buildTable(catalog, meta, tableName, rset);
					result.AddTable(table);
				}
			} catch (Exception e) {
				if (_verbose) {
					Console.Error.WriteLine(e.Message);
					Console.Error.WriteLine(e.StackTrace);
				}
				OutputDevice.Message.WriteLine("Database problem reading meta data: " + e.Message.Trim());
			} finally {
				if (rset != null) {
					try {
						rset.Close();
					} catch (Exception e) {
					}
				}
			}

			return result;
		}

		public Table getTable(SQLSession session, String tableName) {
			Table table = null;
			IDataReader rset = null;
			try {
				String catalog = session.Connection.Database;
				DatabaseMetaData meta = new DatabaseMetaData(session.Connection);
				rset = meta.getColumns(catalog, null, tableName, null);
				table = buildTable(catalog, meta, tableName, rset);
			} catch (Exception e) {
				if (_verbose) {
					Console.Error.WriteLine(e.Message);
					Console.Error.WriteLine(e.StackTrace);
				}
				OutputDevice.Message.WriteLine("Database problem reading meta data: " + e.Message.Trim());
			} finally {
				if (rset != null) {
					try {
						rset.Close();
					} catch (Exception e) {
					}
				}
			}
			return table;
		}

		private Table buildTable(String catalog,
								 DatabaseMetaData meta,
								 String tableName,
								 IDataReader rset) {

			Table table = null;
			if (rset != null) {
				table = new Table(tableName);
				PrimaryKey pk = getPrimaryKey(meta, tableName);
				IDictionary fks = getForeignKeys(meta, tableName);
				// what about the following duplicate?
				// rset = meta.getColumns(catalog, null, tableName, null);
				while (!_interrupted && rset.Read()) {
					String colname = rset.GetString(COLUMN_NAME);
					Column column = new Column(colname);
					column.setType(rset.GetString(TYPE_NAME));
					column.setSize(rset.GetInt32(COLUMN_SIZE));
					bool nullable = (rset.GetInt32(NULLABLE) ==  1) ? true : false;
					column.setNullable(nullable);
					String defaultVal = rset.GetString(COLUMN_DEF);
					column.setDefault((defaultVal != null) ? defaultVal.Trim() : null);
					column.setPosition(rset.GetInt32(ORDINAL_POSITION));
					column.setPkInfo(pk.getColumnPkInfo(colname));
					column.setFkInfo((ColumnFkInfo)fks[colname]);

					table.addColumn(column);
				}
				rset.Close();
			}
			return table;
		}

		private PrimaryKey getPrimaryKey(DatabaseMetaData meta, String tabName) {
			PrimaryKey result = null;
			IDataReader rset = meta.getPrimaryKeys(null, null, tabName);
			if (rset != null) {
				result = new PrimaryKey();
				String pkname = null;
				while (rset.Read()) {
					String col = rset.GetString(PK_DESC_COLUMN_NAME);
					pkname = rset.GetString(PK_DESC_PK_NAME);
					int pkseq = rset.GetInt32(PK_DESC_KEY_SEQ);
					result.addColumn(col, pkname, pkseq);
				}
				rset.Close();
			}
			return result;
		}

		private IDictionary getForeignKeys(DatabaseMetaData meta, String tabName) {
			IDictionary fks = new Hashtable();

			IDataReader rset = null;
			// some jdbc version 2 drivers (connector/j) have problems with foreign keys...
			try {
				rset = meta.getImportedKeys(null, null, tabName);
			} catch (NullReferenceException e) {
				if (_verbose)
					OutputDevice.Message.WriteLine("Database problem reading meta data: " + e);
			}

			if (rset != null) {
				while (rset.Read()) {
					ColumnFkInfo fk = new ColumnFkInfo(rset.GetString(FK_FK_NAME),
													   rset.GetString(FK_PKTABLE_NAME),
													   rset.GetString(FK_PKCOLUMN_NAME));
					String col = rset.GetString(FK_FKCOLUMN_NAME);
					fks[col] = fk;
				}
				rset.Close();
			}
			return fks;
		}

	}
}