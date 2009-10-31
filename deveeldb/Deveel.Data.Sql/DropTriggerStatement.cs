//  
//  DropTriggerStatement.cs
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
	///<summary>
	/// A parsed state container for the <c>DROP TRIGGER</c> statement.
	///</summary>
	public class DropTriggerStatement : Statement {
		/// <summary>
		/// The name of this trigger.
		/// </summary>
		private String trigger_name;


		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		internal override void Prepare() {
			trigger_name = GetString("trigger_name");
		}

		/// <inheritdoc/>
		internal override Table Evaluate() {

			String type = GetString("type");

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			if (type.Equals("callback_trigger")) {
				Connection.DeleteTrigger(trigger_name);
			} else {

				// Convert the trigger into a table name,
				String schema_name = Connection.CurrentSchema;
				TableName t_name = TableName.Resolve(schema_name, trigger_name);
				t_name = Connection.TryResolveCase(t_name);

				ConnectionTriggerManager manager = Connection.ConnectionTriggerManager;
				manager.DropTrigger(t_name.Schema, t_name.Name);

				// Drop the grants for this object
				Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, t_name.ToString());
			}

			// Return '0' if we created the trigger.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}