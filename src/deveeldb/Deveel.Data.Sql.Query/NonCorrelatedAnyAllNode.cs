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

using Deveel.Data;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	[Serializable]
	class NonCorrelatedAnyAllNode : BranchQueryPlanNode {
		public NonCorrelatedAnyAllNode(IQueryPlanNode left, IQueryPlanNode right, ObjectName[] leftColumnNames, SqlExpressionType subQueryType) 
			: base(left, right) {
			LeftColumnNames = leftColumnNames;
			SubQueryType = subQueryType;
		}

		private NonCorrelatedAnyAllNode(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			LeftColumnNames = (ObjectName[])info.GetValue("LeftColumns", typeof(ObjectName[]));
			SubQueryType = (SqlExpressionType) info.GetInt32("SubQueryType");
		}

		public ObjectName[] LeftColumnNames { get; private set; }

		public SqlExpressionType SubQueryType { get; private set; }

		public override ITable Evaluate(IRequest context) {
			// Solve the left branch result
			var leftResult = Left.Evaluate(context);
			// Solve the right branch result
			var rightResult = Right.Evaluate(context);

			// Solve the sub query on the left columns with the right plan and the
			// given operator.
			return leftResult.SelectAnyAllNonCorrelated(LeftColumnNames, SubQueryType, rightResult);
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("LeftColumns", LeftColumnNames, typeof(ObjectName[]));
			info.AddValue("SubQueryType", (int)SubQueryType);
		}
	}
}
