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

using Deveel.Data.Procedures;

namespace Deveel.Data.Sql {
	public sealed class DropFunctionStatement : Statement {
		/// <summary>
		/// The name of the function.
		/// </summary>
		private TableName fun_name;

		protected override void Prepare() {
			String function_name = GetString("function_name");

			// Resolve the function name into a TableName object.    
			String schema_name = Connection.CurrentSchema;
			fun_name = TableName.Resolve(schema_name, function_name);
			fun_name = Connection.TryResolveCase(fun_name);
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Does the schema exist?
			bool ignore_case = Connection.IsInCaseInsensitiveMode;
			SchemaDef schema =
					Connection.ResolveSchemaCase(fun_name.Schema, ignore_case);
			if (schema == null) {
				throw new DatabaseException("Schema '" + fun_name.Schema + "' doesn't exist.");
			} else {
				fun_name = new TableName(schema.Name, fun_name.Name);
			}

			// Does the user have privs to create this function?
			if (!Connection.Database.CanUserDropProcedureObject(context, User, fun_name)) {
				throw new UserAccessException("User not permitted to drop function: " + fun_name);
			}

			// Drop the function
			ProcedureName proc_name = new ProcedureName(fun_name);
			ProcedureManager manager = Connection.ProcedureManager;
			manager.DeleteProcedure(proc_name);

			// Drop the grants for this object
			Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, proc_name.ToString());
			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}