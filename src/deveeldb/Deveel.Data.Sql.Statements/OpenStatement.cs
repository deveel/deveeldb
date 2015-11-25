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
using System.IO;
using System.Linq;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	public sealed class OpenStatement : SqlStatement {
		public OpenStatement(string cursorName) 
			: this(cursorName, new SqlExpression[] {}) {
		}

		public OpenStatement(string cursorName, SqlExpression[] arguments) {
			CursorName = cursorName;
			Arguments = arguments;
		}

		public string CursorName { get; private set; }

		public SqlExpression[] Arguments { get; set; }

		protected override bool IsPreparable {
			get { return true; }
		}

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

		protected override ITable ExecuteStatement(IRequest context) {
			var cursor = context.Query.FindCursor(CursorName);
			if (cursor == null)
				throw new StatementException(String.Format("Cursor '{0}' was not found in the context.", CursorName));

			cursor.Open(context, Arguments);
			return FunctionTable.ResultTable(context, 0);
		}

		#region PreparedSerializer

		internal class PreparedSerializer : ObjectBinarySerializer<OpenStatement> {
			public override void Serialize(OpenStatement obj, BinaryWriter writer) {
				writer.Write(obj.CursorName);

				var argc = obj.Arguments == null ? 0 : obj.Arguments.Length;
				writer.Write(argc);

				if (obj.Arguments != null) {
					for (int i = 0; i < argc; i++) {
						SqlExpression.Serialize(obj.Arguments[i], writer);
					}
				}
			}

			public override OpenStatement Deserialize(BinaryReader reader) {
				var cursorName = reader.ReadString();

				var argc = reader.ReadInt32();
				var args = new SqlExpression[argc];

				for (int i = 0; i < argc; i++) {
					args[i] = SqlExpression.Deserialize(reader);
				}

				return new OpenStatement(cursorName, args);
			}
		}

		#endregion
	}
}
