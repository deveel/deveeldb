//  
//  CompactStatement.cs
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Statement that handles <c>COMPACT</c> SQL command.
	/// </summary>
	public class CompactStatement : Statement {

		/// <summary>
		/// The name the table that we are to update.
		/// </summary>
		private String table_name;

		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		public override void Prepare() {
			table_name = (String)cmd.GetObject("table_name");
		}

		/// <inheritdoc/>
		public override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(database);

			//    TableName tname =
			//                TableName.Resolve(database.getCurrentSchema(), table_name);
			TableName tname = ResolveTableName(table_name, database);
			// Does the table exist?
			if (!database.TableExists(tname)) {
				throw new DatabaseException("Table '" + tname + "' does not exist.");
			}

			// Does the user have privs to compact this tables?
			if (!database.Database.CanUserCompactTableObject(context,
																  user, tname)) {
				throw new UserAccessException(
				   "User not permitted to compact table: " + table_name);
			}

			// Compact the table,
			database.CompactTable(tname);

			// Return '0' if success.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}