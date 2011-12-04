using System;
using System.Collections.Generic;
using System.Text;

namespace Deveel.Data.QueryPlanning {
	/// <summary>
	/// A <see cref="IQueryPlanNode"/> with a single child.
	/// </summary>
	[Serializable]
	public abstract class SingleQueryPlanNode : IQueryPlanNode {
		/// <summary>
		/// The single child node.
		/// </summary>
		private IQueryPlanNode child;

		protected SingleQueryPlanNode(IQueryPlanNode child) {
			this.child = child;
		}

		/// <summary>
		/// Gets the single child node of the plan.
		/// </summary>
		protected IQueryPlanNode Child {
			get { return child; }
		}

		/// <inheritdoc/>
		public abstract Table Evaluate(IQueryContext context);

		/// <inheritdoc/>
		public virtual IList<TableName> DiscoverTableNames(IList<TableName> list) {
			return child.DiscoverTableNames(list);
		}

		/// <inheritdoc/>
		public virtual IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			return child.DiscoverCorrelatedVariables(level, list);
		}

		/// <inheritdoc/>
		public virtual Object Clone() {
			SingleQueryPlanNode node = (SingleQueryPlanNode)MemberwiseClone();
			node.child = (IQueryPlanNode)child.Clone();
			return node;
		}

		public virtual string Title {
			get { return GetType().Name; }
		}

		/// <inheritdoc/>
		public void DebugString(int level, StringBuilder sb) {
			QueryPlanUtil.Indent(level, sb);
			sb.Append(Title);
			sb.Append('\n');
			child.DebugString(level + 2, sb);
		}
	}
}