//  
//  ShowStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.Sql;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Statement that handles <c>SHOW</c> and <c>DESCRIBE</c> sql commands.
	/// </summary>
	public class ShowStatement : Statement {

		// Various show statics,
		internal const int TABLES = 1;
		internal const int STATUS = 2;
		internal const int DESCRIBE_TABLE = 3;
		internal const int CONNECTIONS = 4;
		internal const int PRODUCT = 5;
		internal const int CONNECTION_INFO = 6;

		/// <summary>
		/// The name the table that we are to update.
		/// </summary>
		private String table_name;

		/// <summary>
		/// The type of information that we are to show.
		/// </summary>
		private String show_type;

		/// <summary>
		/// Arguments of the show statement.
		/// </summary>
		private Expression[] args;

		/// <summary>
		/// The search expression for the show statement (where clause).
		/// </summary>
		private SearchExpression where_clause = new SearchExpression();

		/// <summary>
		/// Creates an empty table with the given column names.
		/// </summary>
		/// <param name="d"></param>
		/// <param name="name"></param>
		/// <param name="cols"></param>
		/// <returns></returns>
		static TemporaryTable CreateEmptyTable(Database d, String name, String[] cols) {
			// Describe the given table...
			DataTableColumnDef[] fields = new DataTableColumnDef[cols.Length];
			for (int i = 0; i < cols.Length; ++i) {
				fields[i] = DataTableColumnDef.CreateStringColumn(cols[i]);
			}
			TemporaryTable temp_table = new TemporaryTable(d, name, fields);
			// No entries...
			temp_table.SetupAllSelectableSchemes();
			return temp_table;
		}

		// ---------- Implemented from Statement ----------

		public override void Prepare() {
			// Get the show variables from the query model
			show_type = (String)cmd.GetObject("show");
			show_type = show_type.ToLower();
			table_name = (String)cmd.GetObject("table_name");
			args = (Expression[])cmd.GetObject("args");
			where_clause = (SearchExpression)cmd.GetObject("where_clause");
		}

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);
			Database d = database.Database;

			// Construct an executor for interpreting SQL queries inside here.
			SQLQueryExecutor executor = new SQLQueryExecutor();

			// The table we are showing,
			TemporaryTable show_table;

			try {

				// How we order the result set
				int[] order_set = null;

				if (show_type.Equals("schema")) {

					SQLQuery query = new SQLQuery(
					   "  SELECT \"name\" AS \"schema_name\", " +
					   "         \"type\", " +
					   "         \"other\" AS \"notes\" " +
					   "    FROM SYS_JDBC.ThisUserSchemaInfo " +
					   "ORDER BY \"schema_name\"");
					return executor.Execute(database, query);

				} else if (show_type.Equals("tables")) {

					String current_schema = database.CurrentSchema;

					SQLQuery query = new SQLQuery(
					   "  SELECT \"Tables.TABLE_NAME\" AS \"table_name\", " +
					   "         I_PRIVILEGE_STRING(\"agg_priv_bit\") AS \"user_privs\", " +
					   "         \"Tables.TABLE_TYPE\" as \"table_type\" " +
					   "    FROM SYS_JDBC.Tables, " +
					   "         ( SELECT AGGOR(\"priv_bit\") agg_priv_bit, " +
					   "                  \"object\", \"param\" " +
					   "             FROM SYS_JDBC.ThisUserSimpleGrant " +
					   "            WHERE \"object\" = 1 " +
					   "         GROUP BY \"param\" )" +
					   "   WHERE \"Tables.TABLE_SCHEM\" = ? " +
					   "     AND CONCAT(\"Tables.TABLE_SCHEM\", '.', \"Tables.TABLE_NAME\") = \"param\" " +
					   "ORDER BY \"Tables.TABLE_NAME\"");
					query.AddVariable(current_schema);

					return executor.Execute(database, query);

				} else if (show_type.Equals("status")) {

					SQLQuery query = new SQLQuery(
					   "  SELECT \"stat_name\" AS \"name\", " +
					   "         \"value\" " +
					   "    FROM SYS_INFO.sUSRDatabaseStatistics ");

					return executor.Execute(database, query);

				} else if (show_type.Equals("describe_table")) {

					TableName tname = ResolveTableName(table_name, database);
					if (!database.TableExists(tname)) {
						throw new StatementException(
											"Unable to find table '" + table_name + "'");
					}

					SQLQuery query = new SQLQuery(
					  "  SELECT \"column\" AS \"name\", " +
					  "         i_sql_type(\"type_desc\", \"size\", \"scale\") AS \"type\", " +
					  "         \"not_null\", " +
					  "         \"index_str\" AS \"index\", " +
					  "         \"default\" " +
					  "    FROM SYS_JDBC.ThisUserTableColumns " +
					  "   WHERE \"schema\" = ? " +
					  "     AND \"table\" = ? " +
					  "ORDER BY \"seq_no\" ");
					query.AddVariable(tname.Schema);
					query.AddVariable(tname.Name);

					return executor.Execute(database, query);

				} else if (show_type.Equals("connections")) {

					SQLQuery query = new SQLQuery(
					   "SELECT * FROM SYS_INFO.sUSRCurrentConnections");

					return executor.Execute(database, query);

				} else if (show_type.Equals("product")) {

					SQLQuery query = new SQLQuery(
					   "SELECT \"name\", \"version\" FROM " +
					   "  ( SELECT \"value\" AS \"name\" FROM SYS_INFO.sUSRProductInfo " +
					   "     WHERE \"var\" = 'name' ), " +
					   "  ( SELECT \"value\" AS \"version\" FROM SYS_INFO.sUSRProductInfo " +
					   "     WHERE \"var\" = 'version' ) "
					);

					return executor.Execute(database, query);

				} else if (show_type.Equals("connection_info")) {

					SQLQuery query = new SQLQuery(
					   "SELECT * FROM SYS_INFO.sUSRConnectionInfo"
					);

					return executor.Execute(database, query);

				} else if (show_type.Equals("jdbc_procedures")) {
					// Need implementing?
					show_table = CreateEmptyTable(d, "JDBCProcedures",
						new String[] { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                           "R1", "R2", "R3", "REMARKS", "PROCEDURE_TYPE" });
				} else if (show_type.Equals("jdbc_procedure_columns")) {
					// Need implementing?
					show_table = CreateEmptyTable(d, "JDBCProcedureColumns",
						new String[] { "PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME",
                           "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE",
                           "TYPE_NAME", "PRECISION", "LENGTH", "SCALE",
                           "RADIX", "NULLABLE", "REMARKS" });
				} else if (show_type.Equals("jdbc_catalogs")) {
					// Need implementing?
					show_table = CreateEmptyTable(d, "JDBCCatalogs",
												  new String[] { "TABLE_CAT" });
				} else if (show_type.Equals("jdbc_table_types")) {
					// Describe the given table...
					DataTableColumnDef[] fields = new DataTableColumnDef[1];
					fields[0] = DataTableColumnDef.CreateStringColumn("TABLE_TYPE");

					TemporaryTable temp_table =
									   new TemporaryTable(d, "JDBCTableTypes", fields);
					String[] supported_types = {
            "TABLE", "VIEW", "SYSTEM TABLE",
            "TRIGGER", "FUNCTION", "SEQUENCE" };
					for (int i = 0; i < supported_types.Length; ++i) {
						temp_table.NewRow();
						temp_table.SetRowObject(TObject.GetString(supported_types[i]),
												"JDBCTableTypes.TABLE_TYPE");
					}
					temp_table.SetupAllSelectableSchemes();
					show_table = temp_table;
					order_set = new int[] { 0 };
				} else if (show_type.Equals("jdbc_best_row_identifier")) {
					// Need implementing?
					show_table = CreateEmptyTable(d, "JDBCBestRowIdentifier",
						  new String[] { "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                "PSEUDO_COLUMN" });
				} else if (show_type.Equals("jdbc_version_columns")) {
					// Need implementing?
					show_table = CreateEmptyTable(d, "JDBCVersionColumn",
						  new String[] { "SCOPE", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
                "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS",
                "PSEUDO_COLUMN" });
				} else if (show_type.Equals("jdbc_index_info")) {
					// Need implementing?
					show_table = CreateEmptyTable(d, "JDBCIndexInfo",
						  new String[] { "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME",
                "NON_UNIQUE", "INDEX_QUALIFIER", "INDEX_NAME", "TYPE",
                "ORDINAL_POSITION", "COLUMN_NAME", "ASC_OR_DESC",
                "CARDINALITY", "PAGES", "FILTER_CONDITION"
              });
				} else {
					throw new StatementException("Unknown SHOW identifier: " + show_type);
				}

			} catch (DataException e) {
				throw new DatabaseException("SQL Error: " + e.Message);
			} catch (ParseException e) {
				throw new DatabaseException("Parse Error: " + e.Message);
			} catch (TransactionException e) {
				throw new DatabaseException("Transaction Error: " + e.Message);
			}

			return show_table;
		}
	}
}