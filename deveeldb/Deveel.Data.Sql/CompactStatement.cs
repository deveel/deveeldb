// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Statement that handles <c>COMPACT</c> SQL command.
	/// </summary>
	public class CompactStatement : Statement {
		public CompactStatement(TableName tableName) {
			TableName = tableName;
		}

		public CompactStatement() {
		}

		/// <summary>
		/// The name the table that we are to update.
		/// </summary>
		private String table_name;

		public TableName TableName {
			get { return TableName.Resolve(GetString("table_name")); }
			set {
				if (value == null)
					throw new ArgumentNullException("value");
				SetValue("table_name", value.ToString(false));
			}
		}

		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		protected override void Prepare() {
			table_name = GetString("table_name");
		}

		/// <inheritdoc/>
		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			//    TableName tname =
			//                TableName.Resolve(Connection.CurrentSchema, table_name);
			TableName tname = ResolveTableName(table_name, Connection);
			// Does the table exist?
			if (!Connection.TableExists(tname)) {
				throw new DatabaseException("Table '" + tname + "' does not exist.");
			}

			// Does the user have privs to compact this tables?
			if (!Connection.Database.CanUserCompactTableObject(context,
																  User, tname)) {
				throw new UserAccessException(
				   "User not permitted to compact table: " + table_name);
			}

			// Compact the table,
			Connection.CompactTable(tname);

			// Return '0' if success.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}