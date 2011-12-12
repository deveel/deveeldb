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
		private TableName functionName;

		protected override void Prepare() {
			string functionNameString = GetString("function_name");

			// Resolve the function name into a TableName object.    
			functionName = ResolveTableName(functionNameString);
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Does the schema exist?
			SchemaDef schema = ResolveSchemaName(functionName.Schema);
			if (schema == null)
				throw new DatabaseException("Schema '" + functionName.Schema + "' doesn't exist.");

			functionName = new TableName(schema.Name, functionName.Name);

			// Does the user have privs to create this function?
			if (!Connection.Database.CanUserDropProcedureObject(context, User, functionName))
				throw new UserAccessException("User not permitted to drop function: " + functionName);

			// Drop the function
			ProcedureName proc_name = new ProcedureName(functionName);
			ProcedureManager manager = Connection.ProcedureManager;
			manager.DeleteProcedure(proc_name);

			// Drop the grants for this object
			Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, proc_name.ToString());
			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}