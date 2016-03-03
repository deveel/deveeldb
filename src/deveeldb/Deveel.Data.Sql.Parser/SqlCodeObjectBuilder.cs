// 
//  Copyright 2010-2015 Deveel
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
using System.Linq;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Parser {
	public sealed class SqlCodeObjectBuilder : ISqlNodeVisitor {
		private readonly IList<ISqlCodeObject> objects;
		 
		public SqlCodeObjectBuilder(ITypeResolver typeResolver) {
			TypeResolver = typeResolver;
			objects = new List<ISqlCodeObject>();
		}

		public ITypeResolver  TypeResolver { get; private set; }

		public IEnumerable<ISqlCodeObject> Objects {
			get {
				lock (this) {
					return objects.AsEnumerable();
				}
			}
		}

		void ISqlNodeVisitor.Visit(ISqlNode node) {
			Visit(node);
		}

		internal void AddObject(ISqlCodeObject obj) {
			lock (this) {
				objects.Add(obj);
			}
		}

		private void Visit(ISqlNode node) {
			if (node is ISqlVisitableNode) {
				(node as ISqlVisitableNode).Accept(this);
				return;
			}

			if (node is SequenceOfStatementsNode)
				VisitSequenceOfStatements((SequenceOfStatementsNode) node);
			if (node is SequenceOfBlocksNode)
				VisitSequenceOfBlocks((SequenceOfBlocksNode) node);
		}		

		internal SqlType BuildDataType(DataTypeNode node) {
			return DataTypeBuilder.Build(TypeResolver, node);
		}

		private void VisitSequenceOfStatements(SequenceOfStatementsNode node) {
			foreach (var statementNode in node.Statements) {
				Visit(statementNode);
			}
		}

		private void VisitSequenceOfBlocks(SequenceOfBlocksNode node) {
			foreach (var blockNode in node.BlockNodes) {
				Visit(blockNode);
			}
		}

		public IEnumerable<ISqlCodeObject> Build(ISqlNode rootNode) {
			Visit(rootNode);
			return Objects.ToArray();
		}

		internal static ForeignKeyAction GetForeignKeyAction(string actionName) {
			if (String.Equals("NO ACTION", actionName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals("NOACTION", actionName, StringComparison.OrdinalIgnoreCase))
				return ForeignKeyAction.NoAction;
			if (String.Equals("CASCADE", actionName, StringComparison.OrdinalIgnoreCase))
				return ForeignKeyAction.Cascade;
			if (String.Equals("SET DEFAULT", actionName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals("SETDEFAULT", actionName, StringComparison.OrdinalIgnoreCase))
				return ForeignKeyAction.SetDefault;
			if (String.Equals("SET NULL", actionName, StringComparison.OrdinalIgnoreCase) ||
				String.Equals("SETNULL", actionName, StringComparison.OrdinalIgnoreCase))
				return ForeignKeyAction.SetNull;

			throw new NotSupportedException();
		}
	}
}
