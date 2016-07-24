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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class OpenStatement : SqlStatement, IPlSqlStatement {
		public OpenStatement(string cursorName) 
			: this(cursorName, new SqlExpression[] {}) {
		}

		public OpenStatement(string cursorName, SqlExpression[] arguments) {
			CursorName = cursorName;
			Arguments = arguments;
		}

		private OpenStatement(SerializationInfo info, StreamingContext context) {
			CursorName = info.GetString("CursorName");
			Arguments = (SqlExpression[]) info.GetValue("Arguments", typeof(SqlExpression[]));
		}

		public string CursorName { get; private set; }

		public SqlExpression[] Arguments { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			SqlExpression[] args = null;
			if (Arguments != null) {
				args = (SqlExpression[])Arguments.Clone();

				for (int i = 0; i < args.Length; i++) {
					args[i] = args[i].Prepare(preparer);
				}
			}

			return new OpenStatement(CursorName, args);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			SqlExpression[] args = Arguments;

			var cursor = context.Context.FindCursor(CursorName);
			if (cursor == null)
				throw new StatementException(String.Format("Cursor '{0}' was not found.", CursorName));

			if (args != null) {
				var orderedParams = cursor.CursorInfo.Parameters.OrderBy(x => x.Offset).ToArray();

				if (args.Length != orderedParams.Length)
					throw new StatementException(String.Format("Invalid number of arguments for cursor '{0}' OPEN statement.", CursorName));

				// TODO: Cast to the parameter type here?
			}


			return new OpenStatement(CursorName, args);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("CursorName", CursorName);
			info.AddValue("Arguments", Arguments);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var cursor = context.Request.Context.FindCursor(CursorName);
			if (cursor == null)
				throw new StatementException(String.Format("Cursor '{0}' was not found in the context.", CursorName));

			cursor.Open(Arguments);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.AppendFormat("OPEN {0}", CursorName);

			if (Arguments != null && Arguments.Length > 0) {
				builder.Append("(");
				for (int i = 0; i < Arguments.Length; i++) {
					Arguments[i].AppendTo(builder);

					if (i < Arguments.Length - 1)
						builder.Append(", ");
				}

				builder.Append(")");
			}
		}
	}
}
