// 
//  Copyright 2010-2011  Deveel
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

namespace Deveel.Data {
	public sealed partial class DatabaseConnection {
		internal VariablesManager Variables {
			get { return Transaction.Variables; }
		}

		/// <summary>
		/// Assigns a variable to the expression for the session.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="exp"></param>
		/// <param name="context">A context used to evaluate the expression
		/// forming the value of the variable.</param>
		/// <remarks>
		/// This is a generic way of setting properties of the session.
		/// <para>
		/// Special variables, that are recalled by the system, are:
		/// <list type="bullet">
		/// <item><c>ERROR_ON_DIRTY_SELECT</c>: set to <b>true</b> for turning 
		/// the transaction conflict off on the session.</item>
		/// <item><c>CASE_INSENSITIVE_IDENTIFIERS</c>: <b>true</b> means the grammar 
		/// becomes case insensitive for identifiers resolved by the 
		/// grammar.</item>
		/// </list>
		/// </para>
		/// </remarks>
		public void SetVariable(string name, Expression exp, IQueryContext context) {
			if (name.ToUpper().Equals("ERROR_ON_DIRTY_SELECT")) {
				errorOnDirtySelect = ToBooleanValue(exp);
			} else if (name.ToUpper().Equals("CASE_INSENSITIVE_IDENTIFIERS")) {
				caseInsensitiveIdentifiers = ToBooleanValue(exp);
			} else {
				Transaction.Variables.SetVariable(name, exp, context);
			}
		}

		public Variable DeclareVariable(string name, TType type, bool constant, bool notNull) {
			return Transaction.Variables.DeclareVariable(name, type, constant, notNull);
		}

		public Variable DeclareVariable(string name, TType type, bool notNull) {
			return DeclareVariable(name, type, false, notNull);
		}

		public Variable DeclareVariable(string name, TType type) {
			return DeclareVariable(name, type, false);
		}

		public Variable GetVariable(string name) {
			return Transaction.Variables.GetVariable(name);
		}

		internal void RemoveVariable(string name) {
			Transaction.Variables.RemoveVariable(name);
		}

		/// <inheritdoc cref="Data.Transaction.SetPersistentVariable"/>
		public void SetPersistentVariable(string variable, String value) {
			// Assert
			CheckExclusive();
			Transaction.SetPersistentVariable(variable, value);
		}

		/// <inheritdoc cref="Data.Transaction.GetPersistantVariable"/>
		public String GetPersistentVariable(string variable) {
			return Transaction.GetPersistantVariable(variable);
		}
	}
}