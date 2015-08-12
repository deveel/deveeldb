using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	class ExecutePlanNode {
		public ExecutePlanNode(IExecutable statement) {
			if (statement == null)
				throw new ArgumentNullException("statement");

			Statement = statement;
		}

		private IExecutable Statement { get; set; }

		public ExecutePlanNode Next { get; private set; }

		public ExecutePlanNode Parent { get; private set; }

		public string Label {
			get {
				if (!IsLabeled)
					return null;

				return ((ILabeledExecutable) Statement).Label;
			}
		}

		public bool IsLabeled {
			get { return Statement is ILabeledExecutable; }
		}

		public ExecutePlanNode ChildTree { get; private set; }

		public void Execute(IQueryContext context) {
			Statement.Execute(context);
		}

		public ExecutePlanNode FindLabeled(string label) {
			ExecutePlanNode node = this;
			while (node != null) {
				if (node.IsLabeled &&
				    node.Label.Equals(label))
					return node;

				node = node.Parent;
			}

			if (ChildTree != null) {
				node = ChildTree.FindLabeled(label);

				if (node != null)
					return node;
			}

			node = Next;
			while (node != null) {
				if (node.IsLabeled &&
				    node.Label.Equals(label))
					return node;

				node = node.Next;
			}

			return null;
		}

		public static ExecutePlanNode Build(IEnumerable<SqlStatement> statements) {
			return Build(null, statements);
		}

		public static ExecutePlanNode Build(ExecutePlanNode parentNode, IEnumerable<SqlStatement> statements) {
			var statementList = statements.ToList();
			statementList.Reverse();

			ExecutePlanNode node = null;
			foreach (var statement in statementList) {
				if (node == null) {
					node = new ExecutePlanNode(statement);
				} else {
					var oldNode = node;
					node = new ExecutePlanNode(statement);
					oldNode.Next = node;
				}

				if (parentNode != null)
					node.Parent = parentNode;

				if (statement is IParentExecutable) {
					var children = ((IParentExecutable) statement).Children;
					var childTree = Build(node, children);
					node.ChildTree = childTree;
				}
			}

			return node;
		}
	}
}
