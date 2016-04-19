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
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CursorForLoopStatement : LoopStatement {
		public CursorForLoopStatement(string indexName, string cursorName) {
			if (String.IsNullOrEmpty(indexName))
				throw new ArgumentNullException("indexName");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			IndexName = indexName;
			CursorName = cursorName;
		}

		private CursorForLoopStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			IndexName = info.GetString("Index");
			CursorName = info.GetString("Cursor");
		}

		public string IndexName { get; private set; }

		public string CursorName { get; private set; }

		protected override void BeforeLoop(ExecutionContext context) {
			var cursor = context.Request.Context.FindCursor(CursorName);
			if (cursor == null)
				throw new StatementException(String.Format("Cursor '{0}' was not defined in this scope.", CursorName));
			if (cursor.Status != CursorStatus.Open)
				throw new StatementException(String.Format("The cursor '{0}' is in an invalid status ({1}).", CursorName,
					cursor.Status.ToString().ToUpperInvariant()));

			context.Request.Context.DeclareVariable(IndexName, PrimitiveTypes.BigInt());
			context.Request.Context.SetVariable(IndexName, SqlExpression.Constant(Field.BigInt(0)));

			base.BeforeLoop(context);
		}

		protected override bool Loop(ExecutionContext context) {
			// TODO: Check if it can still enumerate
			var cursor = context.Request.Context.FindCursor(CursorName);

			return base.Loop(context);
		}

		protected override void AfterLoop(ExecutionContext context) {
			var variable = context.Request.Context.FindVariable(IndexName);
			var value = variable.Evaluate(context.Request).Add(Field.BigInt(1));
			context.Request.Context.SetVariable(IndexName, SqlExpression.Constant(value));

			var cursor = context.Request.Context.FindCursor(CursorName);
			cursor.Fetch(context.Request, FetchDirection.Next);

			base.AfterLoop(context);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Index", IndexName);
			info.AddValue("Cursor", CursorName);

			base.GetData(info);
		}
	}
}
