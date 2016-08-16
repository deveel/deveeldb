// 
//  Copyright 2010-2016 Deveel
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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class FetchStatement : SqlStatement, IPlSqlStatement {
		public FetchStatement(string cursorName, FetchDirection direction) 
			: this(cursorName, direction, null) {
		}

		public FetchStatement(string cursorName, FetchDirection direction, SqlExpression offsetExpression) {
			if (offsetExpression != null) {
				if (direction != FetchDirection.Absolute &&
					direction != FetchDirection.Relative)
					throw new ArgumentException("Cannot specify an offset for a FETCH that is not RELATIVE or ABSOLUTE");
			}

			CursorName = cursorName;
			Direction = direction;
			OffsetExpression = offsetExpression;
		}

		private FetchStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			CursorName = info.GetString("CursorName");
			Direction = (FetchDirection) info.GetInt32("Direction");
			OffsetExpression = (SqlExpression) info.GetValue("Offset", typeof(SqlExpression));
		}

		public string CursorName { get; private set; }

		public FetchDirection Direction { get; private set; }

		public SqlExpression OffsetExpression { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var offset = OffsetExpression;
			if (offset != null)
				offset = offset.Prepare(preparer);

			return new FetchStatement(CursorName, Direction, offset);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.Request.Context.CursorExists(CursorName))
				throw new StatementException(String.Format("The cursor '{0}' was not found in the current context.", CursorName));

			var cursor = context.Request.Context.FindCursor(CursorName);
			
			if (cursor == null)
				throw new StatementException(String.Format("The cursor '{0}' was not found in the current context.", CursorName));
			if (cursor.Status == CursorStatus.Closed)
				throw new StatementException(String.Format("The cursor '{0}' was already closed.", CursorName));

			int offset = -1;
			if (OffsetExpression != null)
				offset = OffsetExpression.EvaluateToConstant(context.Request, null);

			var row = cursor.Fetch(Direction, offset);

			if (row != null) {
				var result = new VirtualTable(row.Table, new List<int> {row.RowId.RowNumber});
				context.SetResult(result);
			}
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("CursorName", CursorName);
			info.AddValue("Direction", (int)Direction);
			info.AddValue("Offset", OffsetExpression);
		}
	}
}
