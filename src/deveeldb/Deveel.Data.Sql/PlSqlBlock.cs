using System;
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql {
	public sealed class PlSqlBlock : IExecutable {
		public PlSqlBlock() {
			Declarations = new List<SqlStatement>();
			Statements = new List<SqlStatement>();
			ExceptionHandlers = new List<ExceptionHandler>();
		}

		public string Label { get; set; }

		public bool IsPrepared { get; private set; }

		public SqlExpression ReturnExpression { get; private set; }

		public bool HasReturn { get; private set; }

		public ICollection<SqlStatement> Statements { get; private set; }

		public ICollection<SqlStatement> Declarations { get; private set; }

		public ICollection<ExceptionHandler> ExceptionHandlers { get; private set; }

		private ExecutePlanNode RootNode { get; set; }

		private bool RootNodeChanged { get; set; }

		public IBlockQueryContext CreateContext(IQueryContext parentContext) {
			return new PlSqlBlockQueryContext(parentContext, this);
		}

		private void SetReturn(SqlExpression expression) {
			ReturnExpression = expression;
			HasReturn = true;
		}

		private void SetNextNodeTo(string label) {
			var node = RootNode.FindLabeled(label);
			if (node == null)
				throw new InvalidOperationException();

			RootNode = node;
			RootNodeChanged = true;
		}

		private void Raise(string exceptionName) {
			throw new NotImplementedException();
		}

		public PlSqlBlock Prepare(IExpressionPreparer preparer, IQueryContext context) {
			throw new NotImplementedException();
		}

		public ITable Execute(IQueryContext context) {
			var blockContext = new PlSqlBlockQueryContext(context, this);

			foreach (var declaration in Declarations) {
				declaration.Execute(blockContext);
			}

			var blockNode = new ExecutePlanNode(this);
			RootNode = ExecutePlanNode.Build(blockNode, Statements);

			var node = RootNode;
			while (node != null) {
				node.Execute(context);

				if (RootNodeChanged) {
					node = RootNode;
				} else {
					node = node.Next;
				}
			}

			return FunctionTable.ResultTable(context, 0);
		}

		#region PlSqlBlockQueryContext

		class PlSqlBlockQueryContext : ChildQueryContext, IBlockQueryContext {
			public PlSqlBlockQueryContext(IQueryContext parentContext, PlSqlBlock block) 
				: base(parentContext) {
				Block = block;
			}

			public PlSqlBlock Block { get; private set; }

			public void SetReturn(SqlExpression expression) {
				Block.SetReturn(expression);
			}

			public void Raise(string exceptionName) {
				Block.Raise(exceptionName);
			}

			public void ControlLoop(LoopControlType controlType, string label) {
				throw new NotImplementedException();
			}

			public void GoTo(string label) {
				Block.SetNextNodeTo(label);
			}
		}

		#endregion
	}
}
