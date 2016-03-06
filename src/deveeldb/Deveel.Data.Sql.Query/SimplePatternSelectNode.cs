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
	class SimplePatternSelectNode : SingleQueryPlanNode {
		public SimplePatternSelectNode(IQueryPlanNode child, SqlExpression expression) 
			: base(child) {
			Expression = expression;
		}

		private SimplePatternSelectNode(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			Expression = (SqlExpression) info.GetValue("Expression", typeof(SqlExpression));
		}

		public SqlExpression Expression { get; private set; }

		public override ITable Evaluate(IRequest context) {
			var t = Child.Evaluate(context);
			return t.Select(context, Expression);
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Expression", Expression);
		}
	}
}
