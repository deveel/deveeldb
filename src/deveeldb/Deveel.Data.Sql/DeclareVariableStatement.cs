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
	[Serializable]
	public sealed class DeclareVariableStatement : Statement {
		private string name;
		private bool constant;
		private bool notNull;
		private TType type;
		private Expression defaultValue;

		protected override void Prepare(IQueryContext context) {
			name = GetString("name");
			type = (TType) GetValue("type");
			constant = GetBoolean("constant");
			notNull = GetBoolean("not_null");
			defaultValue = (Expression) GetValue("default");

			if (constant && defaultValue == null)
				throw new InvalidOperationException("A constant variable must specify a default value.");
		}

		protected override Table Evaluate(IQueryContext context) {
			if (context.GetVariable(name) != null)
				throw new InvalidOperationException("The variable '" + name + "' was already defined.");

			try {
				context.DeclareVariable(name, type, constant, notNull);
			} catch (Exception e) {
				context.Debug.Write(DebugLevel.Error, this, "Error while declaring variable: " + e.Message);
				throw;
			}

			try {
				if (defaultValue != null)
					context.SetVariable(name, defaultValue);
			} catch(Exception e) {
				context.RemoveVariable(name);
				context.Debug.WriteException(e);
				throw;
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}