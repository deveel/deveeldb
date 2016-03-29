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
	/// <summary>
	/// The node for performing a simple select operation on a table.
	/// </summary>
	/// <remarks>
	/// The simple select requires a LHS variable, an operator, and an expression 
	/// representing the RHS.
	/// </remarks>
	[Serializable]
	class SimpleSelectNode : SingleQueryPlanNode {
		public SimpleSelectNode(IQueryPlanNode child, ObjectName columnName, SqlExpressionType op, SqlExpression expression)
			: base(child) {
			ColumnName = columnName;
			OperatorType = op;
			Expression = expression;
		}

		private SimpleSelectNode(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ColumnName = (ObjectName) info.GetValue("Column", typeof(ObjectName));
			OperatorType = (SqlExpressionType) info.GetInt32("Operator");
			Expression = (SqlExpression) info.GetValue("Expression", typeof(SqlExpression));
		}

		public ObjectName ColumnName { get; private set; }

		public SqlExpressionType OperatorType { get; private set; }

		public SqlExpression Expression { get; private set; }

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Column", ColumnName);
			info.AddValue("Operator", (int)OperatorType);
			info.AddValue("Expression", Expression);
		}

		public override ITable Evaluate(IRequest context) {
			// Solve the child branch result
			var table = Child.Evaluate(context);

			// The select operation.
			return table.SimpleSelect(context, ColumnName, OperatorType, Expression);
		}
	}
}