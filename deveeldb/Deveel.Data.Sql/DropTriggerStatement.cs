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
		protected override void Prepare() {
			trigger_name = GetString("trigger_name");
		}

		/// <inheritdoc/>
		protected override Table Evaluate() {

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