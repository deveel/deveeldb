using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateViewStatement : SqlStatement {
		public CreateViewStatement(ObjectName viewName, SqlQueryExpression queryExpression) 
			: this(viewName, null, queryExpression) {
		}

		public CreateViewStatement(ObjectName viewName, IEnumerable<string> columnNames, SqlQueryExpression queryExpression) {
			if (viewName == null)
				throw new ArgumentNullException("viewName");
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			ViewName = viewName;
			ColumnNames = columnNames;
			QueryExpression = queryExpression;
		}

		public ObjectName ViewName { get; private set; }

		public IEnumerable<string> ColumnNames { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		public bool ReplaceIfExists { get; set; }

		public override StatementType StatementType {
			get { return StatementType.CreateView;}
		}

		protected override SqlPreparedStatement PrepareStatement(IQueryContext context) {
			var viewName = context.ResolveTableName(ViewName);

			var queryFrom = QueryExpressionFrom.Create(context, QueryExpression);
			var queryPlan = context.DatabaseContext.QueryPlanner().PlanQuery(context, QueryExpression, null);

			var colList = ColumnNames == null ? new string[0] : ColumnNames.ToArray();

			// Wrap the result around a SubsetNode to alias the columns in the
			// table correctly for this view.
			int sz = colList.Length;
			var originalNames = queryFrom.GetResolvedColumns();
			var newColumnNames = new ObjectName[originalNames.Length];

			if (sz > 0) {
				if (sz != originalNames.Length)
					throw new InvalidOperationException("Column list is not the same size as the columns selected.");

				for (int i = 0; i < sz; ++i) {
					var colName = colList[i];
					newColumnNames[i] = new ObjectName(viewName, colName);
				}
			} else {
				sz = originalNames.Length;
				for (int i = 0; i < sz; ++i) {
					newColumnNames[i] = new ObjectName(viewName, originalNames[i].Name);
				}
			}

			// Check there are no repeat column names in the table.
			for (int i = 0; i < sz; ++i) {
				var columnName = newColumnNames[i];
				for (int n = i + 1; n < sz; ++n) {
					if (newColumnNames[n].Equals(columnName))
						throw new InvalidOperationException(String.Format("Duplicate column name '{0}' in view. A view may not contain duplicate column names.", columnName));
				}
			}

			// Wrap the plan around a SubsetNode plan
			queryPlan = new SubsetNode(queryPlan, originalNames, newColumnNames);

			// We have to execute the plan to get the TableInfo that represents the
			// result of the view execution.
			var table = queryPlan.Evaluate(context);
			var tableInfo = table.TableInfo.Alias(viewName);

			return new PreparedCreateView(tableInfo, queryPlan);
		}

		#region PreparedCreateView

		[Serializable]
		class PreparedCreateView : SqlPreparedStatement {
			public PreparedCreateView(TableInfo tableInfo, IQueryPlanNode queryPlan) {
				TableInfo = tableInfo;
				QueryPlan = queryPlan;
			}

			public TableInfo TableInfo { get; private set; }

			public IQueryPlanNode QueryPlan { get; private set; }

			public override ITable Evaluate(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion
	}
}
