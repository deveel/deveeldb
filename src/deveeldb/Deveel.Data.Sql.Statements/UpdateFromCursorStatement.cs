using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class UpdateFromCursorStatement : SqlPreparableStatement {
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

		protected override IPreparedStatement PrepareStatement(IRequest request) {
			var cursor = request.FindCursor(CursorName);
			if (cursor == null)
				throw new ObjectNotFoundException(new ObjectName(CursorName), "The source cursor was not found.");

			var tableName = request.Query.ResolveTableName(TableName);
			if (tableName == null)
				throw new ObjectNotFoundException(TableName);

			var table = request.Query.GetMutableTable(tableName);
			if (table == null)
				throw new ObjectNotFoundException(tableName);

			var columns = table.TableInfo.Select(x => new ObjectName(tableName, x.ColumnName));

			var queryExpression = cursor.QueryExpression;
			var queryFrom = QueryExpressionFrom.Create(request, queryExpression);
			
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
		class Prepared : SqlPreparedStatement {
			protected override void ExecuteStatement(ExecutionContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
