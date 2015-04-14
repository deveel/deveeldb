using System;
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	public sealed class TablePlan {
		public TablePlan(IQueryPlanNode plan, ObjectName[] columnNames, string[] uniqueNames) {
			if (plan == null)
				throw new ArgumentNullException("plan");
			if (columnNames == null)
				throw new ArgumentNullException("columnNames");

			Plan = plan;
			ColumnNames = columnNames;
			UniqueNames = uniqueNames;
			LeftJoinType = JoinType.None;
			RightJoinType = JoinType.None;
			IsUpdated = false;
		}

		/// <summary>
		/// Returns the plan for this table source.
		/// </summary>
		public IQueryPlanNode Plan { get; private set; }

		/// <summary>
		/// Returns true if the planner was updated.
		/// </summary>
		public bool IsUpdated { get; private set; }

		/// <summary>
		/// The list of fully qualified column objects that are accessible 
		/// within this plan.
		/// </summary>
		public ObjectName[] ColumnNames { get; private set; }

		/// <summary>
		/// The list of unique key names of the tables in this plan.
		/// </summary>
		public string[] UniqueNames { get; private set; }

		public TablePlan LeftPlan { get; private set; }

		public TablePlan RightPlan { get; private set; }

		public JoinType LeftJoinType { get; private set; }

		public JoinType RightJoinType { get; private set; }

		public SqlExpression LeftOnExpression { get; private set; }

		public SqlExpression RightOnExpression { get; private set; }

		public void SetCachePoint() {
			if (!(Plan is CachePointNode))
				Plan = new CachePointNode(Plan);
		}

		public void SetUpdated() {
			IsUpdated = true;
		}

		public void LeftJoin(TablePlan left, JoinType joinType, SqlExpression onExpression) {
			LeftPlan = left;
			LeftJoinType = joinType;
			LeftOnExpression = onExpression;
		}

		public void RightJoin(TablePlan right, JoinType joinType, SqlExpression onExpression) {
			RightPlan = right;
			RightJoinType = joinType;
			RightOnExpression = onExpression;
		}

		public void MergeJoin(TablePlan left, TablePlan right) {
			if (left.RightPlan != right) {
				if (left.RightPlan != null) {
					RightJoin(left.RightPlan, left.RightJoinType, left.RightOnExpression);
					RightPlan.LeftPlan = this;
				}
				if (right.LeftPlan != null) {
					LeftJoin(right.LeftPlan, right.LeftJoinType, right.LeftOnExpression);
					LeftPlan.RightPlan = this;
				}
			}

			if (left.LeftPlan != right) {
				if (LeftPlan == null && left.LeftPlan != null) {
					LeftJoin(left.LeftPlan, left.LeftJoinType, left.LeftOnExpression);
					LeftPlan.RightPlan = this;
				}
				if (RightPlan == null && right.RightPlan != null) {
					RightJoin(right.RightPlan, right.RightJoinType, right.RightOnExpression);
					RightPlan.LeftPlan = this;
				}
			}
		}

		public bool ContainsColumn(ObjectName columnName) {
			return ColumnNames.Contains(columnName);
		}

		public bool ContainsName(string name) {
			if (UniqueNames == null || UniqueNames.Length == 0)
				return false;

			return UniqueNames.Contains(name);
		}

		public TablePlan Clone() {
			return new TablePlan(Plan, ColumnNames, UniqueNames);
		}

		public void UpdatePlan(IQueryPlanNode queryPlan) {
			Plan = queryPlan;
			IsUpdated = true;
		}
	}
}
