#if DEBUG
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
		private readonly SqlSession session;
		private readonly string table;
		private readonly string schema;
		private readonly bool caseSensitive;
		private MetaProperty[] meta;
		private IDbCommand workingCommand;
		private string whereClause;

		public TableDumpSource(string schema, string table, bool caseSensitive, SqlSession session) {
			this.session = session;
			this.schema = schema;
			this.table = table;
			this.caseSensitive = caseSensitive;
		}

		public String Description {
			get { return "table '" + table + "'"; }
		}

		public String TableName {
			get { return table; }
		}

		public void SetWhereClause(String value) {
			whereClause = value;
		}

		public IDbCommand Command {
			get { return workingCommand; }
		}

		public MetaProperty[] MetaProperties {
			get {
				if (meta != null)
					return meta;

				ArrayList metaList = new ArrayList();
				DeveelDbConnection conn = session.Connection;
				/*
				 * if the same column is in more than one schema defined, then
				 * oracle seems to write them out twice..
				 */
				ArrayList doubleCheck = new ArrayList();
				System.Data.DataTable columnsTable = conn.GetSchema(DeveelDbMetadataSchemaNames.Columns,
														new string[] { null, schema, table, null });
				for (int i = 0; i < columnsTable.Rows.Count; i++) {
					DataRow row = columnsTable.Rows[i];
					String columnName = row["COLUMN_NAME"].ToString();
					if (doubleCheck.Contains(columnName))
						continue;

					doubleCheck.Add(columnName);
					metaList.Add(new MetaProperty(columnName, (SqlType)row["SQL_DATA_TYPE"]));
				}
				meta = (MetaProperty[])metaList.ToArray(typeof(MetaProperty));
				return meta;
			}
		}

		public IDataReader Reader {
			get {
				StringBuilder selectStmt = new StringBuilder("SELECT ");
				for (int i = 0; i < meta.Length; ++i) {
					MetaProperty p = meta[i];
					if (i != 0)
						selectStmt.Append(", ");
					selectStmt.Append(p.fieldName);
				}

				selectStmt.Append(" FROM ").Append(table);
				if (whereClause != null) {
					selectStmt.Append(" WHERE ").Append(whereClause);
				}
				workingCommand = session.CreateCommand();
				workingCommand.CommandText = selectStmt.ToString();
				return workingCommand.ExecuteReader();
			}
		}

		public long ExpectedRows {
			get {
				CancelWriter selectInfo = new CancelWriter(OutputDevice.Message);
				IDbCommand stmt = null;
				IDataReader rset = null;
				try {
					selectInfo.Write("determining number of rows...");
					stmt = session.CreateCommand();
					StringBuilder countStmt = new StringBuilder("SELECT COUNT(*) FROM ");
					countStmt.Append(table);
					if (whereClause != null) {
						countStmt.Append(" WHERE ");
						countStmt.Append(whereClause);
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
#endif