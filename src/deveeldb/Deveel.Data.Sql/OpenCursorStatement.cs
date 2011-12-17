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
	[Serializable]
	public sealed class OpenCursorStatement : Statement {
		public OpenCursorStatement(TableName cursorName) {
			CursorName = cursorName;
		}

		public OpenCursorStatement() {
		}

		public TableName CursorName {
			get { return TableName.Resolve(GetString("name")); }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				SetValue("name", value.ToString(false));
			}
		}

		protected override Table Evaluate(IQueryContext context) {
			string cursorNameString = GetString("name");
			TableName cursorName = ResolveTableName(context, cursorNameString);

			Cursor cursor = context.GetCursor(cursorName);
			if (cursor == null)
				throw new InvalidOperationException("The cursor '" + cursorNameString + "' was not defined within this transaction.");

			cursor.Open(context);
			return FunctionTable.ResultTable(context, 1);
		}
	}
}