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

		public string IndexName { get; private set; }

		public string CursorName { get; private set; }

		protected override void BeforeLoop(ExecutionContext context) {
			// TODO: define the index variable into the context

			base.BeforeLoop(context);
		}

		protected override bool Loop(ExecutionContext context) {
			// TODO: Get the cursor and check if it is still enumerating

			return base.Loop(context);
		}

		protected override void AfterLoop(ExecutionContext context) {
			// TODO: Get the index variable from the context
			// TODO: Increment the value of the variable
			// TODO: Redefine the value of the variable into the context
			// TODO: Advance the cursor to the next element

			base.AfterLoop(context);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			if (!String.IsNullOrEmpty(Label)) {
				builder.Append("<<{0}>>", Label);
				builder.AppendLine();
			}

			builder.Append("FOR {0} IN {1}", IndexName, CursorName);
			builder.AppendLine();
			builder.Append("LOOP");
			builder.Indent();

			foreach (var statement in Statements) {
				statement.Append(builder);
				builder.AppendLine();
			}

			builder.DeIndent();
			builder.Append("END LOOP");
		}
	}
}
