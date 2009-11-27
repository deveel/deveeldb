//  
//  DeclareVariableStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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

using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	public sealed class DeclareVariableStatement : Statement {
		private string name;
		private bool constant;
		private bool not_null;
		private TType type;
		private Expression default_value;

		internal override void Prepare() {
			name = GetString("name");
			type = (TType) GetValue("type");
			constant = GetBoolean("constant");
			not_null = GetBoolean("not_null");
			default_value = (Expression) GetValue("default");

			if (constant && default_value == null)
				throw new InvalidOperationException("A constant variable must specify a default value.");
		}

		internal override Table Evaluate() {
			DatabaseConnection db = Connection;
			DatabaseQueryContext context = new DatabaseQueryContext(db);

			if (db.GetVariable(name) != null)
				throw new InvalidOperationException("The variable '" + name + "' was already defined.");

			try {
				db.DeclareVariable(name, type, constant, not_null);
			} catch (Exception e) {
				Debug.Write(DebugLevel.Error, this, "Error while declaring variable: " + e.Message);
				throw;
			}

			try {
				if (default_value != null)
					db.SetVariable(name, default_value, context);
			} catch(Exception e) {
				db.RemoveVariable(name);
				Debug.WriteException(e);
				throw;
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}