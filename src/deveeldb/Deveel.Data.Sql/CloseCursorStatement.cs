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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Closes an open cursor within the current execution context.
	/// </summary>
	[Serializable]
	public sealed class CloseCursorStatement : Statement {
		public CloseCursorStatement(string cursorName) {
			SetValue("name", cursorName);
		}

		public CloseCursorStatement() {
		}

		private string name;
		private TableName resolvedName;

		/// <summary>
		/// Gets or sets the qualified name of the curosr to close.
		/// </summary>
		public TableName CursorName {
			get { return TableName.Resolve(GetString("name")); }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				SetValue("name", value.ToString(false));
			}
		}

		protected override void Prepare() {
			name = GetString("name");

			resolvedName = ResolveTableName(name);

			string nameStrip = resolvedName.Name;

			if (nameStrip.IndexOf('.') != -1)
				throw new DatabaseException("Cursor name can not contain '.' character.");

		}

		protected override Table Evaluate() {
			Cursor cursor = Connection.GetCursor(resolvedName);
			if (cursor == null)
				throw new InvalidOperationException("The cursor '" + name + "' was not defined within this transaction.");

			cursor.Close();
			return FunctionTable.ResultTable(QueryContext, 0);
		}
	}
}