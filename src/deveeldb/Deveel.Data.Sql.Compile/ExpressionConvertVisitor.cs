// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Runtime.Remoting.Services;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Compile {
	public class ExpressionConvertVisitor : SqlNodeVisitor {
		public ExpressionConvertVisitor(IExpressionNode inputNode) {
			if (inputNode == null) 
				throw new ArgumentNullException("inputNode");

			InputNode = inputNode;
		}

		public IExpressionNode InputNode { get; private set; }

		public SqlExpression OutputExpression { get; private set; }

		public SqlExpression Convert() {
			VisitNode(InputNode);
			return OutputExpression;
		}

		protected override void VisitNode(ISqlNode node) {
			if (node is IExpressionNode)
				OutputExpression = VisitExpression((IExpressionNode) node);

			base.VisitNode(node);
		}

		private SqlExpression VisitExpression(IExpressionNode expressionNode) {
			if (expressionNode is SqlUnaryExpressionNode)
				return VisitUnaryExpression((SqlUnaryExpressionNode) expressionNode);
			if (expressionNode is SqlBinaryExpressionNode)
				return VisitBinaryExpression((SqlBinaryExpressionNode) expressionNode);
			if (expressionNode is SqlBetweenExpressionNode)
				return VisitBetweenExpression((SqlBetweenExpressionNode) expressionNode);
			if (expressionNode is SqlFunctionCallExpressionNode)
				return VisitFunctionCallExpression((SqlFunctionCallExpressionNode) expressionNode);
			if (expressionNode is SqlConstantExpressionNode)
				return VisitConstantExpression((SqlConstantExpressionNode) expressionNode);
			if (expressionNode is SqlCaseExpressionNode)
				return VisitCaseExpression((SqlCaseExpressionNode) expressionNode);

			throw new NotSupportedException();
		}

		private SqlConditionalExpression VisitCaseExpression(SqlCaseExpressionNode expressionNode) {
			throw new NotImplementedException();
		}

		private SqlConstantExpression VisitConstantExpression(SqlConstantExpressionNode expressionNode) {
			var sqlValue = expressionNode.Value;
			DataObject obj;
			if (sqlValue is SqlString) {
				obj = DataObject.VarChar((SqlString) sqlValue);
			} else if (sqlValue is SqlBoolean) {
				obj = DataObject.Boolean((SqlBoolean) sqlValue);
			} else if (sqlValue is SqlNumber) {
				obj = DataObject.Number((SqlNumber) sqlValue);
			} else {
				throw new NotSupportedException("Constant value is not supported.");
			}

			return SqlExpression.Constant(obj);
		}

		private SqlFunctionCallExpression VisitFunctionCallExpression(SqlFunctionCallExpressionNode expressionNode) {
			throw new NotImplementedException();
		}

		private SqlBinaryExpression VisitBetweenExpression(SqlBetweenExpressionNode expressionNode) {
			var testExp = VisitExpression(expressionNode.Expression);
			var minValue = VisitExpression(expressionNode.MinValue);
			var maxValue = VisitExpression(expressionNode.MaxValue);

			var smallerExp = SqlExpression.SmallerOrEqualThan(testExp, maxValue);
			var greaterExp = SqlExpression.GreaterOrEqualThan(testExp, minValue);
			return SqlExpression.LogicalAnd(smallerExp, greaterExp);
		}

		private SqlBinaryExpression VisitBinaryExpression(SqlBinaryExpressionNode expressionNode) {
			throw new NotImplementedException();
		}

		private SqlUnaryExpression VisitUnaryExpression(SqlUnaryExpressionNode expressionNode) {
			throw new NotImplementedException();
		}
	}
}