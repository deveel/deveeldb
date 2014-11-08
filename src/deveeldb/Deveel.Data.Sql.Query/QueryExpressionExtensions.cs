using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Query {
	static class QueryExpressionExtensions {
		public static IList<ObjectName> DiscoverTableNames(this SqlExpression expression, IList<ObjectName> list) {
			var visitor = new TableNamesVisitor(list);
			visitor.Visit(expression);
			return visitor.TableNames;
		}

		public static IList<QueryReference> DiscoverQueryReferences(this SqlExpression expression, ref int level, IList<QueryReference> list) {
			throw new NotImplementedException();
		}

		#region TableNamesVisitor

		class TableNamesVisitor : SqlExpressionVisitor {
			public TableNamesVisitor(IList<ObjectName> list) {
				TableNames = list;
			}

			public IList<ObjectName> TableNames { get; private set; }

			public override SqlExpression VisitConstant(SqlConstantExpression constant) {
				var value = constant.Value;
				if (!value.IsNull && value.Value is SqlQueryObject &&
					((SqlQueryObject)value.Value).QueryPlan is QueryPlanNode) {
					var queryObject = (SqlQueryObject) value.Value;
					var planNode = (QueryPlanNode) queryObject.QueryPlan;
					TableNames = planNode.DiscoverTableNames(TableNames);
				}

				return base.VisitConstant(constant);
			}
		}

		#endregion

		#region QueryReferencesVisitor

		class QueryReferencesVisitor : SqlExpressionVisitor {
			public QueryReferencesVisitor(IList<QueryReference> list, int level) {
				References = list;
				Level = level;
			}

			public IList<QueryReference> References { get; private set; }

			public int Level { get; private set; }

			public override SqlExpression Visit(SqlExpression expression) {
				if (expression is QueryReferenceExpression)
					VisitQueryReference((QueryReferenceExpression) expression);

				return base.Visit(expression);
			}

			private void VisitQueryReference(QueryReferenceExpression expression) {
				
			}

			public override SqlExpression VisitConstant(SqlConstantExpression constant) {
				var value = constant.Value;
				if (!value.IsNull && value.Value is SqlQueryObject &&
					((SqlQueryObject)value.Value).QueryPlan is QueryPlanNode) {
					var queryObject = (SqlQueryObject)value.Value;
					var planNode = (QueryPlanNode)queryObject.QueryPlan;
					References = planNode.DiscoverQueryReferences(Level, References);
				}

				return base.VisitConstant(constant);
			}
		}

		#endregion
	}
}
