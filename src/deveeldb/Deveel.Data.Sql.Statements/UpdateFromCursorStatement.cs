// 
//  Copyright 2010-2015 Deveel
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
//


using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class UpdateFromCursorStatement : SqlStatement, IPlSqlStatement {
		public UpdateFromCursorStatement(ObjectName tableName, string cursorName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			TableName = tableName;
			CursorName = cursorName;
		}

		public ObjectName TableName { get; private set; }

		public string CursorName { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var cursor = context.FindCursor(CursorName);
			if (cursor == null)
				throw new ObjectNotFoundException(new ObjectName(CursorName), "The source cursor was not found.");

			var tableName = context.Query.Session.SystemAccess.ResolveTableName(TableName);
			if (tableName == null)
				throw new ObjectNotFoundException(TableName);

			var table = context.IsolatedAccess.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);

			var columns = table.TableInfo.Select(x => new ObjectName(tableName, x.ColumnName));

			var queryExpression = cursor.QueryExpression;
			var queryFrom = QueryExpressionFrom.Create(context, queryExpression);
			
			var assignments = new List<SqlColumnAssignment>();
			foreach (var column in columns) {
				// TODO:
			}

			// TODO: get the columns from the table and the columns exposed by the cursor
			//       and then make a set of assignments
			throw new NotImplementedException();
		}

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			protected override void ExecuteStatement(ExecutionContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
