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
	public sealed class FetchStatement : Statement {
		protected override void Prepare() {
		}

		protected override Table Evaluate() {
			string cursorNameString = GetString("name");
			TableName cursorName = ResolveTableName(cursorNameString);

			CursorFetch fetchInfo = (CursorFetch)GetValue("fetch");

			Cursor cursor = Connection.GetCursor(cursorName);
			if (cursor == null)
				throw new InvalidOperationException("The cursor '" + cursorNameString + "' was not defined within this transaction.");

			int offset = -1;
			if (fetchInfo.Offset != null) {
				// we resolve any variable in the expression of the offset
				Expression offsetExpr = (Expression) fetchInfo.Offset.Clone();

				// and finally the value of the offset
				offset = offsetExpr.Evaluate(null, QueryContext);
			}

			if (fetchInfo.Into.HasElements)
				return cursor.FetchInto(fetchInfo.Orientation, offset, QueryContext, fetchInfo.Into);

			// so we finally fetch from the cursor
			return cursor.Fetch(fetchInfo.Orientation, offset);
		}
	}
}