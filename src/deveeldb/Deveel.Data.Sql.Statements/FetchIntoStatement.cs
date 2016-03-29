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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class FetchIntoStatement : SqlStatement {
		public FetchIntoStatement(string cursorName, FetchDirection direction, SqlExpression offsetExpression, SqlExpression referenceExpression) {
			if (offsetExpression != null) {
				if (direction != FetchDirection.Absolute &&
					direction != FetchDirection.Relative)
					throw new ArgumentException("Cannot specify an offset for a FETCH that is not RELATIVE or ABSOLUTE");
			}

			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");
			if (referenceExpression == null)
				throw new ArgumentNullException("referenceExpression");

			CursorName = cursorName;
			Direction = direction;
			OffsetExpression = offsetExpression;
			ReferenceExpression = referenceExpression;
		}

		private FetchIntoStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			CursorName = info.GetString("Cursor");
			Direction = (FetchDirection) info.GetInt32("Direction");
			OffsetExpression = (SqlExpression) info.GetValue("Offset", typeof (SqlExpression));
			ReferenceExpression = (SqlExpression) info.GetValue("Reference", typeof (SqlExpression));
		}

		public string CursorName { get; private set; }

		public FetchDirection Direction { get; private set; }

		public SqlExpression OffsetExpression { get; private set; }

		public SqlExpression ReferenceExpression { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Cursor", CursorName);
			info.AddValue("Direction", (int)Direction);
			info.AddValue("Offset", OffsetExpression);
			info.AddValue("Reference", ReferenceExpression);
		}

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var offset = OffsetExpression;
			if (offset != null)
				offset = offset.Prepare(preparer);

			var reference = ReferenceExpression.Prepare(preparer);
			return new FetchIntoStatement(CursorName, Direction, offset, reference);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var reference = ReferenceExpression;

			if (ReferenceExpression is SqlReferenceExpression) {
				var referenceName = ((SqlReferenceExpression) ReferenceExpression).ReferenceName;
				if (referenceName.Parent == null &&
					context.Context.CursorExists(referenceName.Name)) {
					reference = SqlExpression.VariableReference(referenceName.Name);
				} else {
					var tableName = context.Access.ResolveTableName(referenceName);
					if (!context.Access.TableExists(tableName))
						throw new ObjectNotFoundException(tableName, "Reference table for the FETCH INTO clause was not found.");

					reference = SqlExpression.Reference(tableName);
				}
			}

			return new FetchIntoStatement(CursorName, Direction, OffsetExpression, reference);
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

			var fetchContext = new FetchContext(context.Request, Direction, ReferenceExpression) {
				Offset = offset
			};

			cursor.FetchInto(fetchContext);
		}
	}
}
