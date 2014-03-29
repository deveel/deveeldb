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

using Deveel.Data.DbSystem;
using Deveel.Data.Security;

namespace Deveel.Data.Sql {
	///<summary>
	/// A parsed state container for the <c>DROP TRIGGER</c> statement.
	///</summary>
	[Serializable]
	public class DropTriggerStatement : Statement {
		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		protected override Table Evaluate(IQueryContext context) {
			string triggerNameString = GetString("trigger_name");

			string type = GetString("type");

			if (type.Equals("callback_trigger")) {
				context.Connection.DeleteCallbackTrigger(triggerNameString);
			} else {
				// Convert the trigger into a table name,
				TableName triggerName = ResolveTableName(context, triggerNameString);

				ConnectionTriggerManager manager = context.Connection.TriggerManager;
				manager.DropTrigger(triggerName.Schema, triggerName.Name);

				// Drop the grants for this object
				context.Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, triggerName.ToString());
			}

			// Return '0' if we created the trigger.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}