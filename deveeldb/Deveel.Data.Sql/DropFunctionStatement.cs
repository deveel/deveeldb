// 
//  DropFunctionStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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

namespace Deveel.Data.Sql {
	public sealed class DropFunctionStatement : Statement {
		/// <summary>
		/// The name of the function.
		/// </summary>
		private TableName fun_name;

		public override void Prepare() {
			String function_name = (String)cmd.GetObject("function_name");

			// Resolve the function name into a TableName object.    
			String schema_name = database.CurrentSchema;
			fun_name = TableName.Resolve(schema_name, function_name);
			fun_name = database.TryResolveCase(fun_name);
		}

		public override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(database);

			// Does the schema exist?
			bool ignore_case = database.IsInCaseInsensitiveMode;
			SchemaDef schema =
					database.ResolveSchemaCase(fun_name.Schema, ignore_case);
			if (schema == null) {
				throw new DatabaseException("Schema '" + fun_name.Schema + "' doesn't exist.");
			} else {
				fun_name = new TableName(schema.Name, fun_name.Name);
			}

			// Does the user have privs to create this function?
			if (!database.Database.CanUserDropProcedureObject(context, user, fun_name)) {
				throw new UserAccessException("User not permitted to drop function: " + fun_name);
			}

			// Drop the function
			ProcedureName proc_name = new ProcedureName(fun_name);
			ProcedureManager manager = database.ProcedureManager;
			manager.DeleteProcedure(proc_name);

			// Drop the grants for this object
			database.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, proc_name.ToString());
			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}