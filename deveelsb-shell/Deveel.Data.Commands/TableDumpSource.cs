using System;
using System.Collections;
using System.Data;
using System.Text;

using Deveel.Data.Client;
using Deveel.Data.Shell;
using Deveel.Design;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	class TableDumpSource : IDumpSource {
		private readonly SqlSession _session;
		private readonly String _table;
		private readonly String _schema;
		private readonly bool _caseSensitive;
		private MetaProperty[] _meta;
		private IDbCommand _workingStatement;
		private String _whereClause;

		public TableDumpSource(String schema, String table, bool caseSensitive, SqlSession session) {
			_session = session;
			_schema = schema;
			_table = table;
			_caseSensitive = caseSensitive;
		}

		public String Description {
			get { return "table '" + _table + "'"; }
		}

		public String TableName {
			get { return _table; }
		}

		public void setWhereClause(String whereClause) {
			_whereClause = whereClause;
		}

		public IDbCommand Command {
			get { return _workingStatement; }
		}

		public MetaProperty[] MetaProperties {
			get {
				if (_meta != null)
					return _meta;

				ArrayList metaList = new ArrayList();
				DeveelDbConnection conn = _session.Connection;
				/*
				 * if the same column is in more than one schema defined, then
				 * oracle seems to write them out twice..
				 */
				ArrayList doubleCheck = new ArrayList();
				System.Data.DataTable columnsTable = conn.GetSchema(DeveelDbMetadataSchemaNames.Columns,
														new string[] { null, _schema, _table, null });
				for (int i = 0; i < columnsTable.Rows.Count; i++) {
					DataRow row = columnsTable.Rows[i];
					String columnName = row["COLUMN_NAME"].ToString();
					if (doubleCheck.Contains(columnName))
						continue;
					doubleCheck.Add(columnName);
					metaList.Add(new MetaProperty(columnName, (int)row["DATA_TYPE"]));
				}
				_meta = (MetaProperty[])metaList.ToArray(typeof(MetaProperty));
				return _meta;
			}
		}

		public IDataReader Reader {
			get {
				StringBuilder selectStmt = new StringBuilder("SELECT ");
				for (int i = 0; i < _meta.Length; ++i) {
					MetaProperty p = _meta[i];
					if (i != 0)
						selectStmt.Append(", ");
					selectStmt.Append(p.fieldName);
				}

				selectStmt.Append(" FROM ").Append(_table);
				if (_whereClause != null) {
					selectStmt.Append(" WHERE ").Append(_whereClause);
				}
				_workingStatement = _session.CreateCommand();
				_workingStatement.CommandText = selectStmt.ToString();
				return _workingStatement.ExecuteReader();
			}
		}

		public long ExpectedRows {
			get {
				CancelWriter selectInfo = new CancelWriter(OutputDevice.Message);
				IDbCommand stmt = null;
				IDataReader rset = null;
				try {
					selectInfo.Write("determining number of rows...");
					stmt = _session.CreateCommand();
					StringBuilder countStmt = new StringBuilder("SELECT COUNT(*) FROM ");
					countStmt.Append(_table);
					if (_whereClause != null) {
						countStmt.Append(" WHERE ");
						countStmt.Append(_whereClause);
					}
					stmt.CommandText = countStmt.ToString();
					rset = stmt.ExecuteReader();
					if (!rset.Read())
						return -1;
					return rset.GetInt64(0);
				} catch (Exception) {
					return -1;
				} finally {
					if (rset != null) {
						try {
							rset.Close();
						} catch (Exception) {

						}
					}
					selectInfo.Cancel();
				}
			}
		}
	}
}