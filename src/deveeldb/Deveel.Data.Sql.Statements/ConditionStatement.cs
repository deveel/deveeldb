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

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class ConditionStatement : SqlStatement, IPlSqlStatement {
		public ConditionStatement(SqlExpression condition, SqlStatement[] trueStatements) 
			: this(condition, trueStatements, new SqlStatement[0]) {
		}

		public ConditionStatement(SqlExpression condition, SqlStatement[] trueStatements, SqlStatement[] falseStatements) {
			if (condition == null)
				throw new ArgumentNullException("condition");
			if (trueStatements == null)
				throw new ArgumentNullException("trueStatements");

			ConditionExpression = condition;
			TrueStatements = trueStatements;
			FalseStatements = falseStatements;
		}

		private ConditionStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ConditionExpression = (SqlExpression) info.GetValue("Condition", typeof (SqlExpression));
			TrueStatements = (SqlStatement[]) info.GetValue("True", typeof (SqlStatement[]));
			FalseStatements = (SqlStatement[]) info.GetValue("False", typeof (SqlStatement[]));
		}

		public SqlExpression ConditionExpression { get; private set; }

		public SqlStatement[] TrueStatements { get; private set; }

		public SqlStatement[] FalseStatements { get; set; }

		protected override SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			var condition = ConditionExpression.Prepare(preparer);
			var trueStatements = TrueStatements;
			for (int i = 0; i < trueStatements.Length; i++) {
				trueStatements[i] = trueStatements[i].Prepare(preparer);
			}

			var falseStatements = FalseStatements;
			if (falseStatements != null) {
				for (int i = 0; i < falseStatements.Length; i++) {
					falseStatements[i] = falseStatements[i].Prepare(preparer);
				}
			}

			return new ConditionStatement(condition, trueStatements, falseStatements);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var trueStatements = TrueStatements;
			for (int i = 0; i < trueStatements.Length; i++) {
				trueStatements[i] = trueStatements[i].Prepare(context);
			}

			var falseStatements = FalseStatements;
			if (falseStatements != null) {
				for (int i = 0; i < falseStatements.Length; i++) {
					falseStatements[i] = falseStatements[i].Prepare(context);
				}
			}

			return new ConditionStatement(ConditionExpression, trueStatements, falseStatements);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			var resultType = ConditionExpression.ReturnType(context.Request, null);
			if (!(resultType is BooleanType))
				throw new StatementException("The condition expression does not evaluate to a boolean.");

			// The condition statement triggers the creation of a new context

			var block = context.NewBlock(this);

			var conditionResult = ConditionExpression.EvaluateToConstant(context.Request, null);
			if (conditionResult) {
				foreach (var statement in TrueStatements) {
					statement.Execute(block);

					if (block.HasTermination)
						break;
				}
			} else if (FalseStatements != null) {
				foreach (var statement in FalseStatements) {
					statement.Execute(block);

					if (block.HasTermination)
						break;
				}
			}
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Condition", ConditionExpression);
			info.AddValue("True", TrueStatements);
			if (FalseStatements != null) {
				info.AddValue("False", FalseStatements);
			} else {
				info.AddValue("False", new SqlStatement[0]);
			}
		}
	}
}
