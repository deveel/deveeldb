using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Parser {
	class LoopStatementNode : SqlStatementNode {
		private const string ForLoopType = "for";
		private const string CursorForType = "cursor-for";
		private const string WhileLoopType = "while";
		private const string BasicLoopType = "basic";

		internal LoopStatementNode() {
			LoopType = BasicLoopType;
		}

		public IEnumerable<ISqlNode> Nodes { get; private set; }

		public IExpressionNode ForLowerBound { get; private set; }

		public IExpressionNode ForUpperBound { get; private set; }

		public string ForCursorName { get; private set; }

		public IExpressionNode WhileCondition { get; private set; }

		public string ForIndexName { get; private set; }

		public string LoopType { get; private set; }
		 
		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			LoopBlock loop;

			if (LoopType == BasicLoopType) {
				loop = new LoopBlock();
			} else if (LoopType == ForLoopType) {
				var lowerBound = ExpressionBuilder.Build(ForLowerBound);
				var upperBound = ExpressionBuilder.Build(ForUpperBound);

				loop = new ForLoop(ForIndexName, lowerBound, upperBound);
			} else if (LoopType == CursorForType) {
				loop = new CursorForLoop(ForIndexName, ForCursorName);
			} else if (LoopType == WhileLoopType) {
				var condition = ExpressionBuilder.Build(WhileCondition);

				loop = new WhileLoop(condition);
			} else {
				throw new InvalidOperationException();
			}

			SetObjectsTo(loop, builder.TypeResolver);
			builder.AddObject(loop);
		}

		private void SetObjectsTo(LoopBlock loop, ITypeResolver typeResolver) {
			var objects = new List<ISqlCodeObject>();
			if (Nodes != null) {
				foreach (var statement in Nodes) {
					var subBuilder = new SqlCodeObjectBuilder(typeResolver);
					var sqlCodeObjects = subBuilder.Build(statement);
					objects.AddRange(sqlCodeObjects);
				}
			}

			foreach (var obj in objects) {
				loop.Objects.Add(obj);
			}
		}
	}
}
