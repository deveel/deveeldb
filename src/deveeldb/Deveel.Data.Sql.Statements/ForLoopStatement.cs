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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ForLoopStatement : LoopStatement {
		public ForLoopStatement(string indexName, SqlExpression lowerBound, SqlExpression upperBound) {
			if (String.IsNullOrEmpty(indexName))
				throw new ArgumentNullException("indexName");
			if (lowerBound == null)
				throw new ArgumentNullException("lowerBound");
			if (upperBound == null)
				throw new ArgumentNullException("upperBound");

			IndexName = indexName;
			LowerBound = lowerBound;
			UpperBound = upperBound;
		}

		private ForLoopStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
		}

		public string IndexName { get; private set; }

		public SqlExpression LowerBound { get; private set; }

		public SqlExpression UpperBound { get; private set; }

		public bool Reverse { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var lower = LowerBound.Prepare(preparer);
			var upper = UpperBound.Prepare(preparer);

			return new ForLoopStatement(IndexName, lower, upper);
		}

		protected override void BeforeLoop(ExecutionContext context) {
			context.Request.Context.DeclareVariable(IndexName, PrimitiveTypes.BigInt());
			context.Request.Context.SetVariable(IndexName, SqlExpression.Constant(Field.BigInt(0)));

			base.BeforeLoop(context);
		}

		protected override void AfterLoop(ExecutionContext context) {
			var variable = context.Request.Context.FindVariable(IndexName);
			var value = variable.Value.Add(Field.BigInt(1));
			context.Request.Context.SetVariable(IndexName, SqlExpression.Constant(value));

			base.AfterLoop(context);
		}

		protected override bool Loop(ExecutionContext context) {
			// TODO: Evaluate the upper and lower bound against the context
			// TODO: Evaluate the index and check it is contained within upper and lower bounds
			return base.Loop(context);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			if (!String.IsNullOrEmpty(Label)) {
				builder.Append("<<{0}>>", Label);
				builder.AppendLine();
			}

			builder.Append("FOR {0} ", IndexName);

			if (Reverse)
				builder.Append("REVERSE");

			builder.Append("IN {0}...{1}", LowerBound, UpperBound);

			builder.AppendLine("LOOP");
			builder.Indent();

			foreach (var statement in Statements) {
				statement.Append(builder);
				builder.AppendLine();
			}

			builder.DeIndent();
			builder.AppendLine("END LOOP");
		}
	}
}
