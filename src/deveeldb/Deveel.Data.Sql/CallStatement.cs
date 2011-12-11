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
	/// <summary>
	/// A statement that calls a procedure, and returns a resultant table.
	/// </summary>
	/// <remarks>
	/// This is used to perform some sort of function over the database.  
	/// For example, "CALL SYSTEM_BACKUP('/my_backups/1')" makes a copy 
	/// of the database in the given directory on the disk.
	/// </remarks>
	public class CallStatement : Statement {
		/// <summary>
		/// Constructs the statement with the name of the 
		/// procedure to call and a set of given arguments.
		/// </summary>
		/// <param name="procedure">The name of the procedure 
		/// to call.</param>
		/// <param name="args">The set of arguments to pass to
		/// the procedure called.</param>
		public CallStatement(string procedure, Expression[] args) {
			SetValue("proc_name", procedure);
			SetValue("args", args);
		}

		/// <summary>
		/// Constructs the statement with the name of the 
		/// procedure to call.
		/// </summary>
		/// <param name="procedure">The name of the procedure 
		/// to call.</param>
		public CallStatement(string procedure)
			: this(procedure, null) {
		}

		public CallStatement() {
		}

		/// <summary>
		/// Gets the name of the procedure to call.
		/// </summary>
		public string Procedure {
			get { return GetString("proc_name"); }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");

				SetValue("proc_name", value);
			}
		}

		/// <summary>
		/// Gets the array of the arguments passed to the 
		/// procedure called.
		/// </summary>
		public Expression[] Arguments {
			get { return (Expression[]) GetValue("args"); }
			set { SetValue("args", value); }
		}

		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		protected override void Prepare() {
		}

		/// <inheritdoc/>
		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			String proc_name = GetString("proc_name");
			Expression[] args = (Expression[])GetValue("args");

			// Get the procedure manager
			ProcedureManager manager = Connection.ProcedureManager;
			ProcedureName name;

			TableName p_name = null;

			// If no schema def given in the procedure name, first check for the
			// function in the SYSTEM schema.
			if (proc_name.IndexOf(".") == -1) {
				// Resolve the procedure name into a TableName object.    
				String schema_name = Connection.CurrentSchema;
				TableName tp_name = TableName.Resolve(Database.SystemSchema, proc_name);
				tp_name = Connection.TryResolveCase(tp_name);

				// If exists then use this
				if (manager.ProcedureExists(tp_name)) {
					p_name = tp_name;
				}
			}

			if (p_name == null) {
				// Resolve the procedure name into a TableName object.    
				String schema_name = Connection.CurrentSchema;
				TableName tp_name = TableName.Resolve(schema_name, proc_name);
				tp_name = Connection.TryResolveCase(tp_name);

				// Does the schema exist?
				bool ignore_case = Connection.IsInCaseInsensitiveMode;
				SchemaDef schema =
							Connection.ResolveSchemaCase(tp_name.Schema, ignore_case);
				if (schema == null) {
					throw new DatabaseException("Schema '" + tp_name.Schema + "' doesn't exist.");
				} else {
					tp_name = new TableName(schema.Name, tp_name.Name);
				}

				// If this doesn't exist then generate the error
				if (!manager.ProcedureExists(tp_name)) {
					throw new DatabaseException("Stored procedure '" + proc_name + "' was not found.");
				}

				p_name = tp_name;
			}

			// Does the procedure exist in the system schema?
			name = new ProcedureName(p_name);

			// Check the user has privs to use this stored procedure
			if (!Connection.Database.CanUserExecuteStoredProcedure(context, User, name.ToString())) {
				throw new UserAccessException("User not permitted to call: " + proc_name);
			}

			// Evaluate the arguments
			TObject[] vals = new TObject[args.Length];
			for (int i = 0; i < args.Length; ++i) {
				if (args[i].IsConstant) {
					vals[i] = args[i].Evaluate(null, null, context);
				} else {
					throw new StatementException("CALL argument is not a constant: " + args[i].Text);
				}
			}

			// Invoke the procedure
			TObject result = manager.InvokeProcedure(name, vals);

			// Return the result of the procedure,
			return FunctionTable.ResultTable(context, result);
		}
	}
}