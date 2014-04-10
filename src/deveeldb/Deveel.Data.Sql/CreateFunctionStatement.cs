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

using Deveel.Data.DbSystem;
using Deveel.Data.Procedures;
using Deveel.Data.Security;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class CreateFunctionStatement : Statement {
		public CreateFunctionStatement(TableName functionName, IList args, TType returnType, string location) {
			FunctionName = functionName;

			if (args != null) {
				foreach (object arg in args)
					Arguments.Add(arg);
			}

			ReturnType = returnType;
			Location = location;
		}

		public CreateFunctionStatement(TableName functionName, TType returnType, string location)
			: this(functionName, null, returnType, location) {
		}

		public CreateFunctionStatement() {
		}

		/// <summary>
		/// The name of the function.
		/// </summary>
		private TableName resolvedFunctionName;

		public TableName FunctionName {
			get { return TableName.Resolve(GetString("function_name")); }
			set {
				if (value == null)
					throw new ArgumentNullException("value");
				SetValue("function_name", value.ToString(false));
			}
		}

		public IList Arguments {
			get { return GetList("args", true); }
		}

		public TType ReturnType {
			get { return (TType) GetValue("return_type"); }
			set {
				if (value == null)
					throw new ArgumentException("value");
				SetValue("return_type", value);
			}
		}

		public string Location {
			get { return GetString("location_name"); }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");
				SetValue("location_name", value);
			}
		}

		private void RemoveArgument(string name) {
			int argIndex = -1;
			IList argNames = GetList("arg_names");
			int sz = argNames.Count;

			for (int i = sz - 1; i >= 0; i--) {
				string argName = (string)argNames[i];
				if (argName == name) {
					argNames.RemoveAt(i);
					argIndex = i;
					break;
				}
			}

			if (argIndex != -1) {
				GetList("arg_types").RemoveAt(argIndex);
			}
		}

		protected override bool OnListAdd(string key, object value, ref object newValue) {
			if (key == "args") {
				FunctionArgument arg = value as FunctionArgument;
				if (arg == null)
					throw new ArgumentException();

				GetList("arg_names").Add(arg.Name);
				GetList("arg_types").Add(arg.Type);
			}

			return base.OnListAdd(key, value, ref newValue);
		}

		protected override bool OnListRemoved(string key, object value) {
			if (key == "args") {
				FunctionArgument arg = (FunctionArgument) value;
				RemoveArgument(arg.Name);
			}

			return base.OnListRemoved(key, value);
		}

		protected override bool OnListRemoveAt(string key, int index) {
			if (key == "args") {
				FunctionArgument arg = (FunctionArgument) GetList("args")[index];
				RemoveArgument(arg.Name);
			}
			return base.OnListRemoveAt(key, index);
		}

		protected override bool OnListClear(string key) {
			if (key == "args") {
				GetList("arg_names").Clear();
				GetList("arg_types").Clear();
			}

			return base.OnListClear(key);
		}

		protected override bool OnListInsert(string key, int index, object value, ref object newValue) {
			if (key == "args")	//TODO:
				throw new NotSupportedException("Inserting not yet supported for the arguments list.");

			return base.OnListInsert(key, index, value, ref newValue);
		}

		protected override bool OnListSet(string key, int index, object value, ref object newValue) {
			if (key == "args")	//TODO:
				throw new NotSupportedException("Settingnot yet supported for the arguments list.");

			return base.OnListSet(key, index, value, ref newValue);
		}

		protected override void Prepare(IQueryContext context) {
			string functionName = GetString("function_name");

			// Resolve the function name into a TableName object.
			resolvedFunctionName = ResolveTableName(context, functionName);
		}

		protected override Table Evaluate(IQueryContext context) {
			// Does the schema exist?
			SchemaDef schema = ResolveSchemaName(context, resolvedFunctionName.Schema);
			if (schema == null)
				throw new DatabaseException("Schema '" + resolvedFunctionName.Schema + "' doesn't exist.");

			resolvedFunctionName = new TableName(schema.Name, resolvedFunctionName.Name);

			// Does the user have privs to create this function?
			if (!context.Connection.Database.CanUserCreateProcedureObject(context, resolvedFunctionName))
				throw new UserAccessException("User not permitted to create function: " + resolvedFunctionName);

			// Does a table already exist with this name?
			if (context.Connection.TableExists(resolvedFunctionName))
				throw new DatabaseException("Database object with name '" + resolvedFunctionName + "' already exists.");

			// Get the information about the function we are creating
			IList argNames = GetList("arg_names");
			IList argTypes = GetList("arg_types");
			TObject locName = (TObject)GetValue("location_name");
			TType returnType = (TType)GetValue("return_type");

			// Note that we currently ignore the arg_names list.


			// Convert arg types to an array
			TType[] argTypeArray = new TType[argTypes.Count];
			argTypes.CopyTo(argTypeArray, 0);

			// We must parse the location name into a class name, and method name
			string specification = locName.Object.ToString();
			// Resolve the csharp_specification to an invokation method.
			MethodInfo procMethod = ProcedureManager.GetProcedureMethod(specification, argTypeArray);
			if (procMethod == null) {
				throw new DatabaseException("Unable to find invokation method for " +
				                            ".NET stored procedure name: " + specification);
			}

			// Convert the information into an easily digestible form.
			ProcedureName procName = new ProcedureName(resolvedFunctionName);
			int sz = argTypes.Count;
			TType[] argList = new TType[sz];
			for (int i = 0; i < sz; ++i) {
				argList[i] = (TType)argTypes[i];
			}

			// Create the .NET function,
			ProcedureManager manager = context.Connection.ProcedureManager;
			manager.DefineProcedure(procName, specification, returnType, argList, context.UserName);

			// The initial grants for a procedure is to give the user who created it
			// full access.
			context.Connection.GrantManager.Grant(
				 Privileges.ProcedureAll, GrantObject.Table,
				 procName.ToString(), context.UserName, true,
				 User.SystemName);

			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}