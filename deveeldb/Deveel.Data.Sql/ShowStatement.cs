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

		// ---------- Implemented from Statement ----------

		internal override void Prepare() {
			// Get the show variables from the command model
			show_type = GetString("show");
			show_type = show_type.ToLower();
			table_name = GetString("table_name");
			args = (Expression[])GetValue("args");
			where_clause = (SearchExpression)GetValue("where_clause");
		}

		internal override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);
			Database d = Connection.Database;

			// Construct an executor for interpreting SQL queries inside here.
			SqlCommandExecutor executor = new SqlCommandExecutor();

			// The table we are showing,
			// TemporaryTable show_table;

			try {

				// How we order the result set
				int[] order_set = null;

				if (show_type.Equals("schema")) {

					SqlCommand command = new SqlCommand(
					   "  SELECT \"name\" AS \"schema_name\", " +
					   "         \"type\", " +
					   "         \"other\" AS \"notes\" " +
					   "    FROM INFORMATION_SCHEMA.ThisUserSchemaInfo " +
					   "ORDER BY \"schema_name\"");
					return executor.Execute(Connection, command);

				} else if (show_type.Equals("tables")) {

					String current_schema = Connection.CurrentSchema;

					SqlCommand command = new SqlCommand(
					   "  SELECT \"Tables.TABLE_NAME\" AS \"table_name\", " +
					   "         I_PRIVILEGE_STRING(\"agg_priv_bit\") AS \"user_privs\", " +
					   "         \"Tables.TABLE_TYPE\" as \"table_type\" " +
					   "    FROM INFORMATION_SCHEMA.Tables, " +
					   "         ( SELECT AGGOR(\"priv_bit\") agg_priv_bit, " +
					   "                  \"object\", \"param\" " +
					   "             FROM INFORMATION_SCHEMA.ThisUserSimpleGrant " +
					   "            WHERE \"object\" = 1 " +
					   "         GROUP BY \"param\" )" +
					   "   WHERE \"Tables.TABLE_SCHEMA\" = ? " +
					   "     AND CONCAT(\"Tables.TABLE_SCHEMA\", '.', \"Tables.TABLE_NAME\") = \"param\" " +
					   "ORDER BY \"Tables.TABLE_NAME\"");
					command.AddVariable(current_schema);

					return executor.Execute(Connection, command);

				} else if (show_type.Equals("status")) {

					SqlCommand command = new SqlCommand(
					   "  SELECT \"stat_name\" AS \"name\", " +
					   "         \"value\" " +
					   "    FROM SYSTEM.sUSRDatabaseStatistics ");

					return executor.Execute(Connection, command);

				} else if (show_type.Equals("describe_table")) {

					TableName tname = ResolveTableName(table_name, Connection);
					if (!Connection.TableExists(tname)) {
						throw new StatementException(
											"Unable to find table '" + table_name + "'");
					}

					SqlCommand command = new SqlCommand(
					  "  SELECT \"column\" AS \"name\", " +
					  "         i_sql_type(\"type_desc\", \"size\", \"scale\") AS \"type\", " +
					  "         \"not_null\", " +
					  "         \"index_str\" AS \"index\", " +
					  "         \"default\" " +
					  "    FROM INFORMATION_SCHEMA.ThisUserTableColumns " +
					  "   WHERE \"schema\" = ? " +
					  "     AND \"table\" = ? " +
					  "ORDER BY \"seq_no\" ");
					command.AddVariable(tname.Schema);
					command.AddVariable(tname.Name);

					return executor.Execute(Connection, command);

				} else if (show_type.Equals("connections")) {

					SqlCommand command = new SqlCommand(
					   "SELECT * FROM SYSTEM.sUSRCurrentConnections");

					return executor.Execute(Connection, command);

				} else if (show_type.Equals("product")) {

					SqlCommand command = new SqlCommand(
					   "SELECT \"name\", \"version\" FROM " +
					   "  ( SELECT \"value\" AS \"name\" FROM SYSTEM.sUSRProductInfo " +
					   "     WHERE \"var\" = 'name' ), " +
					   "  ( SELECT \"value\" AS \"version\" FROM SYSTEM.sUSRProductInfo " +
					   "     WHERE \"var\" = 'version' ) "
					);

					return executor.Execute(Connection, command);

				} else if (show_type.Equals("connection_info")) {

					SqlCommand command = new SqlCommand(
					   "SELECT * FROM SYSTEM.sUSRConnectionInfo"
					);

					return executor.Execute(Connection, command);

				} 
				 else {
					throw new StatementException("Unknown SHOW identifier: " + show_type);
				}

			} catch (DataException e) {
				throw new DatabaseException("SQL Error: " + e.Message);
			} catch (ParseException e) {
				throw new DatabaseException("Parse Error: " + e.Message);
			} catch (TransactionException e) {
				throw new DatabaseException("Transaction Error: " + e.Message);
			}

			// return show_table;
		}
	}
}