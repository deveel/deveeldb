// 
//  Copyright 2010-2014 Deveel
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
using System.Data;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql {
	///<summary>
	/// The SQL <c>SET</c> statement.
	///</summary>
	/// <remarks>
	/// Sets properties within the current local database connection 
	/// such as auto-commit mode.
	/// </remarks>
	[Serializable]
	public class SetStatement : Statement {
		/// <summary>
		/// The type of set this is.
		/// </summary>
		private String type;

		/// <summary>
		/// The variable name of this set statement.
		/// </summary>
		private String var_name;

		/// <summary>
		/// The Expression that is the value to assign the variable 
		/// to (if applicable).
		/// </summary>
		private Expression exp;

		/// <summary>
		/// The value to assign the value to (if applicable).
		/// </summary>
		private String value;



		// ---------- Implemented from Statement ----------

		protected override void Prepare(IQueryContext context) {
			type = GetString("type");
			var_name = GetString("var_name");
			exp = GetExpression("exp");
			value = GetString("value");
		}

		protected override Table Evaluate(IQueryContext context) {
			String com = type.ToLower();

			if (com.Equals("varset")) {
				context.SetVariable(var_name, exp);
			} else if (com.Equals("isolationset")) {
				context.Connection.TransactionIsolation = (IsolationLevel) Enum.Parse(typeof(IsolationLevel), value, true);
			} else if (com.Equals("autocommit")) {
				value = value.ToLower();
				if (value.Equals("on") ||
					value.Equals("1")) {
					context.Connection.AutoCommit = true;
				} else if (value.Equals("off") ||
						 value.Equals("0")) {
					context.Connection.AutoCommit = false;
				} else {
					throw new DatabaseException("Unrecognised value for SET AUTO COMMIT");
				}
			} else if (com.Equals("schema")) {
				// It's particularly important that this is done during exclusive
				// Lock because SELECT requires the schema name doesn't change in
				// mid-process.

				// Change the connection to the schema
				context.Connection.SetDefaultSchema(value);

			} else {
				throw new DatabaseException("Unrecognised set command.");
			}

			return FunctionTable.ResultTable(context, 0);

		}
	}
}