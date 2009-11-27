//  
//  SetStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;

namespace Deveel.Data.Sql {
	///<summary>
	/// The SQL <c>SET</c> statement.
	///</summary>
	/// <remarks>
	/// Sets properties within the current local database connection 
	/// such as auto-commit mode.
	/// </remarks>
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

		internal override void Prepare() {
			type = GetString("type");
			var_name = GetString("var_name");
			exp = GetExpression("exp");
			value = GetString("value");
		}

		internal override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			String com = type.ToLower();

			if (com.Equals("varset")) {
				Connection.SetVariable(var_name, exp, context);
			} else if (com.Equals("isolationset")) {
				Connection.TransactionIsolation = (IsolationLevel) Enum.Parse(typeof(IsolationLevel), value, true);
			} else if (com.Equals("autocommit")) {
				value = value.ToLower();
				if (value.Equals("on") ||
					value.Equals("1")) {
					Connection.AutoCommit = true;
				} else if (value.Equals("off") ||
						 value.Equals("0")) {
					Connection.AutoCommit = false;
				} else {
					throw new DatabaseException("Unrecognised value for SET AUTO COMMIT");
				}
			} else if (com.Equals("schema")) {
				// It's particularly important that this is done during exclusive
				// Lock because SELECT requires the schema name doesn't change in
				// mid-process.

				// Change the connection to the schema
				Connection.SetDefaultSchema(value);

			} else {
				throw new DatabaseException("Unrecognised set command.");
			}

			return FunctionTable.ResultTable(context, 0);

		}
	}
}