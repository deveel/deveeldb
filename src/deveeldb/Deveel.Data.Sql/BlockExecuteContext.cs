using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	public abstract class BlockExecuteContext : ILabeledExecutable {
		internal BlockExecuteContext(PlSqlBlock block, IEnumerable<SqlStatement> statements) {
			Block = block;
			Statements = statements;
		}

		public PlSqlBlock Block { get; private set; }

		string ILabeledExecutable.Label {
			get { return Block.Label; }
		}

		private IEnumerable<SqlStatement> Statements { get; set; }

		private ExecutePlanNode RootNode { get; set; }

		protected abstract IQueryContext CreateQueryContext(IQueryContext parentContext);

		public ITable Execute(IQueryContext context) {
			var blockContext = CreateQueryContext(context);

			var blockNode = new ExecutePlanNode(this);
			RootNode = ExecutePlanNode.Build(blockNode, Statements);

			var node = RootNode;
			while (node != null) {
				node.Execute(blockContext);

				if (RootNodeChanged) {
					node = RootNode;
				} else {
					node = node.Next;
				}
			}

			return FunctionTable.ResultTable(context, 0);
		}

		private bool RootNodeChanged { get; set; }

		internal void SetNextNodeTo(string label) {
			var node = RootNode.FindLabeled(label);
			if (node == null)
				throw new InvalidOperationException(string.Format(
					"Cannot find any statement labeled '{0}' in the current context.", label));

			RootNode = node;
			RootNodeChanged = true;
		}
	}
}
