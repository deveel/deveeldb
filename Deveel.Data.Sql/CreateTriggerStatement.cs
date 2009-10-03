// 
//  CreateTriggerStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A parsed state container for the <c>CREATE TRIGGER</c> statement.
	/// </summary>
	public class CreateTriggerStatement : Statement {

		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		public override void Prepare() {
		}

		/// <inheritdoc/>
		public override Table Evaluate() {

			String trigger_name = (String)cmd.GetObject("trigger_name");
			String type = (String)cmd.GetObject("type");
			String table_name = (String)cmd.GetObject("table_name");
			IList types = (IList)cmd.GetObject("trigger_types");

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			TableName tname = TableName.Resolve(database.CurrentSchema,
												table_name);

			if (type.Equals("callback_trigger")) {
				// Callback trigger - notifies the client when an event on a table
				// occurs.
				if (types.Count > 1) {
					throw new DatabaseException(
						  "Multiple triggered types not allowed for callback triggers.");
				}

				String trig_type = ((String)types[0]).ToUpper();
				TriggerEventType int_type;
				if (trig_type.Equals("INSERT")) {
					int_type = TriggerEventType.Insert;
				} else if (trig_type.Equals("DELETE")) {
					int_type = TriggerEventType.Delete;
				} else if (trig_type.Equals("UPDATE")) {
					int_type = TriggerEventType.Update;
				} else {
					throw new DatabaseException("Unknown trigger type: " + trig_type);
				}

				database.CreateTrigger(trigger_name, tname.ToString(), int_type);

			} else if (type.Equals("procedure_trigger")) {

				// Get the procedure manager
				ProcedureManager proc_manager = database.ProcedureManager;

				String before_after = (String)cmd.GetObject("before_after");
				String procedure_name = (String)cmd.GetObject("procedure_name");
				Expression[] procedure_args =
										 (Expression[])cmd.GetObject("procedure_args");

				// Convert the trigger into a table name,
				String schema_name = database.CurrentSchema;
				TableName t_name = TableName.Resolve(schema_name, trigger_name);
				t_name = database.TryResolveCase(t_name);

				// Resolve the procedure name into a TableName object.    
				TableName t_p_name = TableName.Resolve(schema_name, procedure_name);
				t_p_name = database.TryResolveCase(t_p_name);

				// Does the procedure exist in the system schema?
				ProcedureName p_name = new ProcedureName(t_p_name);

				// Check the trigger name doesn't clash with any existing database object.
				if (database.TableExists(t_name)) {
					throw new DatabaseException("A database object with name '" + t_name +
												"' already exists.");
				}

				// Check the procedure exists.
				if (!proc_manager.ProcedureExists(p_name)) {
					throw new DatabaseException("Procedure '" + p_name +
												"' could not be found.");
				}

				// Resolve the listening type
				int listen_type = 0;
				if (before_after.Equals("before")) {
					listen_type |= TableModificationEvent.BEFORE;
				} else if (before_after.Equals("after")) {
					listen_type |= TableModificationEvent.AFTER;
				} else {
					throw new ApplicationException("Unknown before/after type.");
				}

				for (int i = 0; i < types.Count; ++i) {
					String trig_type = (String)types[i];
					if (trig_type.Equals("insert")) {
						listen_type |= TableModificationEvent.INSERT;
					} else if (trig_type.Equals("delete")) {
						listen_type |= TableModificationEvent.DELETE;
					} else if (trig_type.Equals("update")) {
						listen_type |= TableModificationEvent.UPDATE;
					}
				}

				// Resolve the procedure arguments,
				TObject[] vals = new TObject[procedure_args.Length];
				for (int i = 0; i < procedure_args.Length; ++i) {
					vals[i] = procedure_args[i].Evaluate(null, null, context);
				}

				// Create the trigger,
				ConnectionTriggerManager manager = database.ConnectionTriggerManager;
				manager.CreateTableTrigger(t_name.Schema, t_name.Name,
										   listen_type, tname, p_name.ToString(), vals);

				// The initial grants for a trigger is to give the user who created it
				// full access.
				database.GrantManager.Grant(
					 Privileges.ProcedureAll, GrantObject.Table,
					 t_name.ToString(), user.UserName, true,
					 Database.InternalSecureUsername);

			} else {
				throw new Exception("Unknown trigger type.");
			}

			// Return success
			return FunctionTable.ResultTable(context, 0);
		}
	}
}