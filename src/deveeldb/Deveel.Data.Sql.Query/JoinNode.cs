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

using Deveel.Data;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class JoinNode : BranchQueryPlanNode {
		public JoinNode(IQueryPlanNode left, IQueryPlanNode right, ObjectName leftColumnName, SqlExpressionType @operator, SqlExpression rightExpression) 
			: base(left, right) {
			LeftColumnName = leftColumnName;
			Operator = @operator;
			RightExpression = rightExpression;
		}

		private JoinNode(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			LeftColumnName = (ObjectName)info.GetValue("LeftColumn", typeof(ObjectName));
			Operator = (SqlExpressionType)info.GetInt32("Operator");
			RightExpression = (SqlExpression) info.GetValue("RightExpression", typeof(SqlExpression));
		}

		public ObjectName LeftColumnName { get; private set; }

		public SqlExpressionType Operator { get; private set; }

		public SqlExpression RightExpression { get; private set; }

		public override ITable Evaluate(IRequest context) {
			// Solve the left branch result
			var leftResult = Left.Evaluate(context);
			// Solve the right branch result
			var rightResult = Right.Evaluate(context);

			return leftResult.Join(context, rightResult, LeftColumnName, Operator, RightExpression);
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("LeftColumn", LeftColumnName);
			info.AddValue("Operator", (int)Operator);
			info.AddValue("RightExpression", RightExpression);
		}
	}
}
