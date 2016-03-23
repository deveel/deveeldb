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

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class WhileLoopStatement : LoopStatement {
		public WhileLoopStatement(SqlExpression conditionExpression) {
			if (conditionExpression == null)
				throw new ArgumentNullException("conditionExpression");

			ConditionExpression = conditionExpression;
		}

		private WhileLoopStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ConditionExpression = (SqlExpression) info.GetValue("Condition", typeof (SqlExpression));
		}

		public SqlExpression ConditionExpression { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Condition", ConditionExpression);
			base.GetData(info);
		}

		protected override bool Loop(ExecutionContext context) {
			// TODO: evaluate the condition against the context and return a boolean
			return base.Loop(context);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			if (!String.IsNullOrEmpty(Label)) {
				builder.Append("<<{0}>>", Label);
				builder.AppendLine();
			}

			builder.Append("WHILE {0}", ConditionExpression);
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
