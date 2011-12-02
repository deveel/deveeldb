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

using Deveel.Diagnostics;

namespace Deveel.Data.Sql {
	public sealed class DeclareVariableStatement : Statement {
		private string name;
		private bool constant;
		private bool not_null;
		private TType type;
		private Expression default_value;

		protected override void Prepare() {
			name = GetString("name");
			type = (TType) GetValue("type");
			constant = GetBoolean("constant");
			not_null = GetBoolean("not_null");
			default_value = (Expression) GetValue("default");

			if (constant && default_value == null)
				throw new InvalidOperationException("A constant variable must specify a default value.");
		}

		protected override Table Evaluate() {
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