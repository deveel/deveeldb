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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Parser {
	public sealed class StatementBuilder : SqlNodeVisitor {
		private readonly IUserTypeResolver typeResolver;
		private readonly List<SqlStatement> statements;

		public StatementBuilder() 
			: this(null) {
		}

		public StatementBuilder(IUserTypeResolver typeResolver) {
			this.typeResolver = typeResolver;
			statements = new List<SqlStatement>();
		}

		private SqlExpression Expression(IExpressionNode node) {
			var visitor = new ExpressionBuilder();
			return visitor.Build(node);
		}

		public override void Visit(ISqlNode node) {
			if (node is CreateTableNode)
				VisitCreateTable((CreateTableNode) node);
			if (node is CreateViewNode)
				VisitCreateView((CreateViewNode) node);
			if (node is CreateTriggerNode)
				VisitCreateTrigger((CreateTriggerNode) node);

			if (node is AlterTableNode)
				VisitAlterTable((AlterTableNode) node);

			if (node is SelectStatementNode)
				VisitSelect((SelectStatementNode) node);

			if (node is SequenceOfStatementsNode)
				VisitSequenceOfStatements((SequenceOfStatementsNode) node);
		}

		private void VisitSequenceOfStatements(SequenceOfStatementsNode node) {
			foreach (var statementNode in node.Statements) {
				Visit(statementNode);
			}
		}

		public override void VisitSelect(SelectStatementNode node) {
			var queryExpression = (SqlQueryExpression)Expression(node.QueryExpression);
			var statement = new SqlSelectStatement(queryExpression);
			statements.Add(statement);
		}

		public override void VisitCreateTrigger(CreateTriggerNode node) {
			
		}

		public override void VisitCreateView(CreateViewNode node) {
			var queryExpression = (SqlQueryExpression)Expression(node.QueryExpression);
			var statement = new CreateViewStatement(node.ViewName.Name, node.ColumnNames, queryExpression);
			statements.Add(statement);
		}

		public override void VisitCreateTable(CreateTableNode node) {
			CreateTable.Build(typeResolver, node, statements);
		}

		public IEnumerable<SqlStatement> Build(ISqlNode rootNode, SqlQuery query) {
			Visit(rootNode);
			return statements.AsReadOnly();
		}

		public override void VisitAlterTable(AlterTableNode node) {
			AlterTable.Build(typeResolver, node, statements);
		}

		public IEnumerable<SqlStatement> Build(ISqlNode rootNode, string query) {
			return Build(rootNode, new SqlQuery(query));
		}

		#region CreateTable

		static class CreateTable {
			public static void Build(IUserTypeResolver typeResolver, CreateTableNode node, ICollection<SqlStatement> statements) {
				string idColumn = null;

				var dataTypeBuilder = new DataTypeBuilder();

				var tableName = node.TableName;
				var objTableName = ObjectName.Parse(tableName.Name);
				var constraints = new List<ConstraintInfo>();
				var columns = new List<SqlTableColumn>();

				var expBuilder = new ExpressionBuilder();

				foreach (var column in node.Columns) {
					var dataType = dataTypeBuilder.Build(typeResolver, column.DataType);

					var columnInfo = new SqlTableColumn(column.ColumnName.Text, dataType);

					if (column.Default != null)
						columnInfo.DefaultExpression = expBuilder.Build(column.Default);

					if (column.IsIdentity) {
						if (!String.IsNullOrEmpty(idColumn))
							throw new InvalidOperationException(String.Format("Table {0} defines already {1} as identity column.",
								node.TableName, idColumn));

						if (column.Default != null)
							throw new InvalidOperationException(String.Format("The identity column {0} cannot have a DEFAULT constraint.",
								idColumn));

						idColumn = column.ColumnName.Text;

						columnInfo.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
							new[] {SqlExpression.Constant(node.TableName.Name)});
					}

					foreach (var constraint in column.Constraints) {
						if (String.Equals(ConstraintTypeNames.Check, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
							var exp = expBuilder.Build(constraint.CheckExpression);
							constraints.Add(ConstraintInfo.Check(objTableName, exp, column.ColumnName.Text));
						} else if (String.Equals(ConstraintTypeNames.ForeignKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
							var fTable = ObjectName.Parse(constraint.ReferencedTable.Name);
							var fColumn = constraint.ReferencedColumn.Text;
							var fkey = ConstraintInfo.ForeignKey(objTableName, column.ColumnName.Text, fTable, fColumn);
							if (!String.IsNullOrEmpty(constraint.OnDeleteAction))
								fkey.OnDelete = GetForeignKeyAction(constraint.OnDeleteAction);
							if (!String.IsNullOrEmpty(constraint.OnUpdateAction))
								fkey.OnUpdate = GetForeignKeyAction(constraint.OnUpdateAction);

							constraints.Add(fkey);
						} else if (String.Equals(ConstraintTypeNames.PrimaryKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
							constraints.Add(ConstraintInfo.PrimaryKey(objTableName, column.ColumnName.Text));
						} else if (String.Equals(ConstraintTypeNames.UniqueKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
							constraints.Add(ConstraintInfo.Unique(objTableName, column.ColumnName.Text));
						} else if (String.Equals(ConstraintTypeNames.NotNull, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
							columnInfo.IsNotNull = true;
						} else if (String.Equals(ConstraintTypeNames.Null, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
							columnInfo.IsNotNull = false;
						}
					}

					columns.Add(columnInfo);
				}

				foreach (var constraint in node.Constraints) {
					if (String.Equals(ConstraintTypeNames.Check, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
						var exp = expBuilder.Build(constraint.CheckExpression);
						constraints.Add(ConstraintInfo.Check(constraint.ConstraintName, objTableName, exp, constraint.Columns.ToArray()));
					} else if (String.Equals(ConstraintTypeNames.PrimaryKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
						constraints.Add(ConstraintInfo.PrimaryKey(constraint.ConstraintName, objTableName, constraint.Columns.ToArray()));
					} else if (String.Equals(ConstraintTypeNames.UniqueKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
						constraints.Add(ConstraintInfo.Unique(constraint.ConstraintName, objTableName, constraint.Columns.ToArray()));
					} else if (String.Equals(ConstraintTypeNames.ForeignKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
						var fTable = ObjectName.Parse(constraint.ReferencedTableName.Name);
						var fColumns = constraint.ReferencedColumns;
						var fkey = ConstraintInfo.ForeignKey(constraint.ConstraintName, objTableName, constraint.Columns.ToArray(), fTable,
							fColumns.ToArray());
						if (!String.IsNullOrEmpty(constraint.OnDeleteAction))
							fkey.OnDelete = GetForeignKeyAction(constraint.OnDeleteAction);
						if (!String.IsNullOrEmpty(constraint.OnUpdateAction))
							fkey.OnUpdate = GetForeignKeyAction(constraint.OnUpdateAction);

						constraints.Add(fkey);
					}
				}

				//TODO: Optimization: merge same constraints

				statements.Add(MakeCreateTable(tableName.Name, columns, node.IfNotExists, node.Temporary));

				foreach (var constraint in constraints) {
					statements.Add(MakeAlterTableAddConstraint(objTableName, constraint));
				}
			}

			private static ForeignKeyAction GetForeignKeyAction(string actionName) {
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

			private static SqlStatement MakeAlterTableAddConstraint(ObjectName tableName, ConstraintInfo constraint) {
				var action = new AddConstraintAction(constraint);

				return new SqlAlterTableStatement(tableName,new List<IAlterTableAction>{action});
			}

			private static SqlStatement MakeCreateTable(string tableName, IEnumerable<SqlTableColumn> columns, bool ifNotExists, bool temporary) {
				var tree = new CreateTableStatement(tableName, columns.ToList());
				tree.IfNotExists = ifNotExists;
				tree.Temporary = temporary;
				return tree;
			}
		}

		#endregion

		#region AlterTable

		class AlterTable {
			public static void Build(IUserTypeResolver typeResolver, AlterTableNode node, ICollection<SqlStatement> statements) {
				// TODO:
			}
		}

		#endregion
	}
}
