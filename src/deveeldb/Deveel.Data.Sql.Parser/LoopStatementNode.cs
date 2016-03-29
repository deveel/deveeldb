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
		 
		protected override void BuildStatement(SqlStatementBuilder builder) {
			LoopStatement loop;

			if (LoopType == BasicLoopType) {
				loop = new LoopStatement();
			} else if (LoopType == ForLoopType) {
				var lowerBound = ExpressionBuilder.Build(ForLowerBound);
				var upperBound = ExpressionBuilder.Build(ForUpperBound);

				loop = new ForLoopStatement(ForIndexName, lowerBound, upperBound);
			} else if (LoopType == CursorForType) {
				loop = new CursorForLoopStatement(ForIndexName, ForCursorName);
			} else if (LoopType == WhileLoopType) {
				var condition = ExpressionBuilder.Build(WhileCondition);

				loop = new WhileLoopStatement(condition);
			} else {
				throw new InvalidOperationException();
			}

			SetObjectsTo(loop, builder.TypeResolver);
			builder.AddObject(loop);
		}

		private void SetObjectsTo(LoopStatement loop, ITypeResolver typeResolver) {
			var objects = new List<SqlStatement>();
			if (Nodes != null) {
				foreach (var statement in Nodes) {
					var subBuilder = new SqlStatementBuilder(typeResolver);
					var sqlCodeObjects = subBuilder.Build(statement);
					objects.AddRange(sqlCodeObjects);
				}
			}

			foreach (var obj in objects) {
				loop.Statements.Add(obj);
			}
		}
	}
}
