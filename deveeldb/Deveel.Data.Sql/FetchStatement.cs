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
	public sealed class FetchStatement : Statement {
		/// <summary>
		/// The name of the cursor from where to fetch.
		/// </summary>
		private TableName resolved_name;

		private string name;

		private CursorFetch fetch_info;

		protected override void Prepare() {
			DatabaseConnection db = Connection;

			name = GetString("name");

			string schema_name = db.CurrentSchema;
			resolved_name = TableName.Resolve(schema_name, name);

			string name_strip = resolved_name.Name;

			if (name_strip.IndexOf('.') != -1)
				throw new DatabaseException("Cursor name can not contain '.' character.");

			fetch_info = (CursorFetch) GetValue("fetch");
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			Cursor cursor = Connection.GetCursor(resolved_name);
			if (cursor == null)
				throw new InvalidOperationException("The cursor '" + name + "' was not defined within this transaction.");

			int offset = -1;
			if (fetch_info.Offset != null) {
				// we resolve any variable in the expression of the offset
				Expression offsetExpr = (Expression) fetch_info.Offset.Clone();
				ResolveExpression(offsetExpr);

				// and finally the value of the offset
				offset = (int) offsetExpr.Evaluate(null, context);
			}

			// so we finally fetch from the cursor
			return cursor.Fetch(fetch_info.Orientation, offset);
		}
	}
}