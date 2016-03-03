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

using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Parser {
	class AlterTableNode : SqlStatementNode {
		public string TableName { get; private set; }

		public IEnumerable<IAlterActionNode> Actions { get; private set; }

		public CreateTableNode CreateTable { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is ObjectNameNode) {
				TableName = ((ObjectNameNode) node).Name;
			} else if (node is CreateTableNode) {
				CreateTable = (CreateTableNode) node;
			} else if (node.NodeName == "alter_actions") {
				Actions = node.FindNodes<IAlterActionNode>();
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			if (CreateTable != null) {
				var statements = new List<ISqlCodeObject>();
				CreateTable.Build(builder.TypeResolver, builder);

				foreach (var statement in statements) {
					if (statement is CreateTableStatement)
						((CreateTableStatement) statement).IfNotExists = true;
				}

				foreach (var statement in statements) {
					builder.AddObject(statement);
				}
			} else if (Actions != null) {
				foreach (var action in Actions) {
					BuildAction(builder.TypeResolver, ObjectName.Parse(TableName), action, builder);
				}
			}
		}

		private static void BuildAction(ITypeResolver typeResolver, ObjectName tableName, IAlterActionNode action, SqlCodeObjectBuilder builder) {
			if (action is AddColumnNode) {
				var column = ((AddColumnNode) action).Column;
				var constraints = new List<SqlTableConstraint>();
				var columnInfo = column.BuildColumn(typeResolver, tableName.FullName, constraints);

				builder.AddObject(new AlterTableStatement(tableName, new AddColumnAction(columnInfo)));

				foreach (var constraint in constraints) {
					builder.AddObject(new AlterTableStatement(tableName, new AddConstraintAction(constraint)));
				}
			} else if (action is AddConstraintNode) {
				var constraint = ((AddConstraintNode) action).Constraint;

				var constraintInfo = constraint.BuildConstraint();
				builder.AddObject(new AlterTableStatement(tableName, new AddConstraintAction(constraintInfo)));
			} else if (action is DropColumnNode) {
				var columnName = ((DropColumnNode) action).ColumnName;
				builder.AddObject(new AlterTableStatement(tableName, new DropColumnAction(columnName)));
			} else if (action is DropConstraintNode) {
				var constraintName = ((DropConstraintNode) action).ConstraintName;
				builder.AddObject(new AlterTableStatement(tableName, new DropConstraintAction(constraintName)));
			} else if (action is SetDefaultNode) {
				var actionNode = ((SetDefaultNode) action);
				var columnName = actionNode.ColumnName;
				var expression = ExpressionBuilder.Build(actionNode.Expression);
				builder.AddObject(new AlterTableStatement(tableName, new SetDefaultAction(columnName, expression)));
			} else if (action is DropDefaultNode) {
				var columnName = ((DropDefaultNode) action).ColumnName;
				builder.AddObject(new AlterTableStatement(tableName, new DropDefaultAction(columnName)));
			} else if (action is AlterColumnNode) {
				var column = ((AlterColumnNode) action).Column;
				var constraints = new List<SqlTableConstraint>();
				var columnInfo = column.BuildColumn(typeResolver, tableName.FullName, constraints);

				// CHECK: Here we do a drop and add column: is there a better way on the back-end?
				builder.AddObject(new AlterTableStatement(tableName, new DropColumnAction(columnInfo.ColumnName)));

				builder.AddObject(new AlterTableStatement(tableName, new AddColumnAction(columnInfo)));

				foreach (var constraint in constraints) {
					builder.AddObject(new AlterTableStatement(tableName, new AddConstraintAction(constraint)));
				}
			}
		}
	}
}
