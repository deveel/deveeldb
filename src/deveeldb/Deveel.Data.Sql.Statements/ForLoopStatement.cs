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

			var loop = new ForLoopStatement(IndexName, lower, upper) {Reverse = Reverse};
			foreach (var statement in Statements) {
				loop.Statements.Add(statement);
			}
			return loop;
		}

		protected override LoopStatement CreateNew() {
			return new ForLoopStatement(IndexName, LowerBound, UpperBound) { Reverse = Reverse };
		}

		protected override void BeforeLoop(ExecutionContext context) {
			context.Request.Context.DeclareVariable(IndexName, PrimitiveTypes.BigInt());
			context.Request.Context.SetVariable(IndexName, SqlExpression.Constant(Field.BigInt(0)));

			base.BeforeLoop(context);
		}

		protected override void AfterLoop(ExecutionContext context) {
			var variable = context.Request.Context.FindVariable(IndexName);
			var value = variable.Evaluate(context.Request).Add(Field.BigInt(1));
			context.Request.Context.SetVariable(IndexName, SqlExpression.Constant(value));

			base.AfterLoop(context);
		}

		protected override bool Loop(ExecutionContext context) {
			var variable = context.Request.Context.FindVariable(IndexName);
			var upperBound = ((SqlConstantExpression) UpperBound).Value;
			var lowerBound = ((SqlConstantExpression) LowerBound).Value;

			if (Reverse)
				return variable.Evaluate(context.Request) >= lowerBound;

			return variable.Evaluate(context.Request) < upperBound;
		}
	}
}
