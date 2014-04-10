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
using System.Collections;

using Deveel.Data.DbSystem;
using Deveel.Data.Procedures;
using Deveel.Data.Security;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A parsed state container for the <c>CREATE TRIGGER</c> statement.
	/// </summary>
	[Serializable]
	public class CreateTriggerStatement : Statement {

		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		protected override Table Evaluate(IQueryContext context) {
			string triggerNameString = GetString("trigger_name");
			string typeString = GetString("type");
			string tableNameString = GetString("table_name");
			IList types = GetList("trigger_types");

			TableName tname = ResolveTableName(context, tableNameString);

			if (typeString.Equals("callback_trigger")) {
				// Callback trigger - notifies the client when an event on a table
				// occurs.

				TriggerEventType eventType = new TriggerEventType();

				foreach (string trigType in types) {
					if (trigType.Equals("INSERT", StringComparison.InvariantCultureIgnoreCase)) {
						eventType |= TriggerEventType.Insert;
					} else if (trigType.Equals("DELETE", StringComparison.InvariantCultureIgnoreCase)) {
						eventType |= TriggerEventType.Delete;
					} else if (trigType.Equals("UPDATE", StringComparison.InvariantCultureIgnoreCase)) {
						eventType |= TriggerEventType.Update;
					} else {
						throw new DatabaseException("Unknown trigger type: " + trigType);
					}
				}

				context.Connection.CreateCallbackTrigger(triggerNameString, tname, eventType);

			} else if (typeString.Equals("procedure_trigger")) {

				// Get the procedure manager
				ProcedureManager procManager = context.Connection.ProcedureManager;

				string beforeAfter = GetString("before_after");
				string procNameString = GetString("procedure_name");
				Expression[] procedureArgs = (Expression[])GetValue("procedure_args");

				// Convert the trigger into a table name,
				TableName triggerName = ResolveTableName(context, triggerNameString);

				// Resolve the procedure name into a TableName object.    
				TableName procTableName = ResolveTableName(context, procNameString);

				// Does the procedure exist in the system schema?
				ProcedureName procName = new ProcedureName(procTableName);

				// Check the trigger name doesn't clash with any existing database object.
				if (context.Connection.TableExists(triggerName))
					throw new DatabaseException("A database object with name '" + triggerName +
					                            "' already exists.");

				// Check the procedure exists.
				if (!procManager.ProcedureExists(procName))
					throw new DatabaseException("Procedure '" + procName + "' could not be found.");

				// Resolve the listening type
				TriggerEventType listenType = new TriggerEventType();
				if (beforeAfter.Equals("before")) {
					listenType |= TriggerEventType.Before;
				} else if (beforeAfter.Equals("after")) {
					listenType |= TriggerEventType.After;
				} else {
					throw new ApplicationException("Unknown before/after type.");
				}

				for (int i = 0; i < types.Count; ++i) {
					string trigeventType = (String)types[i];
					if (trigeventType.Equals("insert", StringComparison.InvariantCultureIgnoreCase)) {
						listenType |= TriggerEventType.Insert;
					} else if (trigeventType.Equals("delete", StringComparison.InvariantCultureIgnoreCase)) {
						listenType |= TriggerEventType.Delete;
					} else if (trigeventType.Equals("update", StringComparison.InvariantCultureIgnoreCase)) {
						listenType |= TriggerEventType.Update;
					}
				}

				// Resolve the procedure arguments,
				TObject[] vals = new TObject[procedureArgs.Length];
				for (int i = 0; i < procedureArgs.Length; ++i) {
					vals[i] = procedureArgs[i].Evaluate(null, null, context);
				}

				// Create the trigger,
				ConnectionTriggerManager manager = context.Connection.TriggerManager;
				manager.CreateTableTrigger(triggerName.Schema, triggerName.Name, listenType, tname, procName.ToString(), vals);

				// The initial grants for a trigger is to give the user who created it
				// full access.
				context.Connection.GrantManager.Grant(
					 Privileges.ProcedureAll, GrantObject.Table,
					 triggerName.ToString(), context.UserName, true,
					 User.SystemName);

			} else {
				throw new Exception("Unknown trigger type.");
			}

			// Return success
			return FunctionTable.ResultTable(context, 0);
		}
	}
}