//  
//  FetchStatement.cs
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

namespace Deveel.Data.Sql {
	public sealed class FetchStatement : Statement {
		/// <summary>
		/// The name of the cursor from where to fetch.
		/// </summary>
		private TableName resolved_name;

		private string name;

		private CursorFetch fetch_info;

		internal override void Prepare() {
			DatabaseConnection db = Connection;

			name = GetString("name");

			string schema_name = db.CurrentSchema;
			resolved_name = TableName.Resolve(schema_name, name);

			string name_strip = resolved_name.Name;

			if (name_strip.IndexOf('.') != -1)
				throw new DatabaseException("Cursor name can not contain '.' character.");

			fetch_info = (CursorFetch) GetValue("fetch");
		}

		internal override Table Evaluate() {
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
				offset = offsetExpr.Evaluate(null, context);
			}

			// so we finally fetch from the cursor
			return cursor.Fetch(fetch_info.Orientation, offset);
		}
	}
}