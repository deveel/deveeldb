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
using System.Reflection;

using Deveel.Data.Procedures;

namespace Deveel.Data.Sql {
	public sealed class CreateFunctionStatement : Statement {
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
			SchemaDef schema = Connection.ResolveSchemaCase(fun_name.Schema, ignore_case);
			if (schema == null) {
				throw new DatabaseException("Schema '" + fun_name.Schema + "' doesn't exist.");
			} else {
				fun_name = new TableName(schema.Name, fun_name.Name);
			}

			// Does the user have privs to create this function?
			if (!Connection.Database.CanUserCreateProcedureObject(context, User, fun_name)) {
				throw new UserAccessException("User not permitted to create function: " + fun_name);
			}

			// Does a table already exist with this name?
			if (Connection.TableExists(fun_name)) {
				throw new DatabaseException("Database object with name '" + fun_name +
											"' already exists.");
			}

			// Get the information about the function we are creating
			IList arg_names = GetList("arg_names");
			IList arg_types = GetList("arg_types");
			TObject loc_name = (TObject)GetValue("location_name");
			TType return_type = (TType)GetValue("return_type");

			// Note that we currently ignore the arg_names list.


			// Convert arg types to an array
			TType[] arg_type_array = new TType[arg_types.Count];
			arg_types.CopyTo(arg_type_array, 0);

			// We must parse the location name into a class name, and method name
			String specification = loc_name.Object.ToString();
			// Resolve the java_specification to an invokation method.
			MethodInfo proc_method = ProcedureManager.GetProcedureMethod(specification, arg_type_array);
			if (proc_method == null) {
				throw new DatabaseException("Unable to find invokation method for " +
				                            ".NET stored procedure name: " + specification);
			}

			// Convert the information into an easily digestible form.
			ProcedureName proc_name = new ProcedureName(fun_name);
			int sz = arg_types.Count;
			TType[] arg_list = new TType[sz];
			for (int i = 0; i < sz; ++i) {
				arg_list[i] = (TType)arg_types[i];
			}

			// Create the .NET function,
			ProcedureManager manager = Connection.ProcedureManager;
			manager.DefineProcedure(proc_name, specification, return_type, arg_list, User.UserName);

			// The initial grants for a procedure is to give the user who created it
			// full access.
			Connection.GrantManager.Grant(
				 Privileges.ProcedureAll, GrantObject.Table,
				 proc_name.ToString(), User.UserName, true,
				 Database.InternalSecureUsername);

			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}