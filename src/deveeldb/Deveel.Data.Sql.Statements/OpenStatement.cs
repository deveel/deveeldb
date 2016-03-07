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
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class OpenStatement : SqlStatement, IPreparable, IPreparableStatement {
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

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			SqlExpression[] args = null;
			if (Arguments != null) {
				args = (SqlExpression[])Arguments.Clone();

				for (int i = 0; i < args.Length; i++) {
					args[i] = args[i].Prepare(preparer);
				}
			}

			return new OpenStatement(CursorName, args);
		}

		IStatement IPreparableStatement.Prepare(IRequest context) {
			SqlExpression[] args = Arguments;

			var cursor = context.Query.FindCursor(CursorName);
			if (cursor == null)
				throw new StatementPrepareException(String.Format("Cursor '{0}' was not found.", CursorName));

			if (args != null) {
				var orderedParams = cursor.CursorInfo.Parameters.OrderBy(x => x.Offset).ToArray();

				if (args.Length != orderedParams.Length)
					throw new StatementPrepareException(String.Format("Invalid number of arguments for cursor '{0}' OPEN statement.", CursorName));

				// TODO: Cast to the parameter type here?
			}


			return new OpenStatement(CursorName, args);
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("CursorName", CursorName);
			info.AddValue("Arguments", Arguments);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var cursor = context.Request.Query.FindCursor(CursorName);
			if (cursor == null)
				throw new StatementException(String.Format("Cursor '{0}' was not found in the context.", CursorName));

			cursor.Open(context.Request, Arguments);
		}
	}
}
