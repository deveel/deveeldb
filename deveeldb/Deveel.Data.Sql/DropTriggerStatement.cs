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
		public override void Prepare() {
			trigger_name = (String)cmd.GetObject("trigger_name");
		}

		/// <inheritdoc/>
		public override Table Evaluate() {

			String type = (String)cmd.GetObject("type");

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			if (type.Equals("callback_trigger")) {
				database.DeleteTrigger(trigger_name);
			} else {

				// Convert the trigger into a table name,
				String schema_name = database.CurrentSchema;
				TableName t_name = TableName.Resolve(schema_name, trigger_name);
				t_name = database.TryResolveCase(t_name);

				ConnectionTriggerManager manager = database.ConnectionTriggerManager;
				manager.DropTrigger(t_name.Schema, t_name.Name);

				// Drop the grants for this object
				database.GrantManager.RevokeAllGrantsOnObject(
											  GrantObject.Table, t_name.ToString());
			}

			// Return '0' if we created the trigger.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}