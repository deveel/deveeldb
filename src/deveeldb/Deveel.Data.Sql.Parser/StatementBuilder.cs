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

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Parser {
	class StatementBuilder {
		private readonly ITypeResolver typeResolver;
		private readonly List<SqlStatement> statements;

		public StatementBuilder() 
			: this(null) {
		}

		public StatementBuilder(ITypeResolver typeResolver) {
			this.typeResolver = typeResolver;
			statements = new List<SqlStatement>();
		}

		private SqlExpression Expression(IExpressionNode node) {
			return ExpressionBuilder.Build(node);
		}

		public void Build(ISqlNode node) {
			if (node is CreateSchemaNode)
				BuildCreateSchema((CreateSchemaNode) node);

			if (node is CreateTableNode)
				BuildCreateTable((CreateTableNode) node);
			if (node is CreateViewNode)
				BuildCreateView((CreateViewNode) node);
			if (node is CreateTriggerNode)
				BuildCreateTrigger((CreateTriggerNode) node);
			if (node is CreateSequenceNode)
				BuildCreateSequence((CreateSequenceNode) node);

			if (node is AlterTableNode)
				BuildAlterTable((AlterTableNode) node);

			if (node is SelectStatementNode)
				BuildSelect((SelectStatementNode) node);

			if (node is UpdateStatementNode)
				BuildUpdate((UpdateStatementNode)node);
			if (node is InsertStatementNode)
				BuildInsert((InsertStatementNode) node);

			if (node is DropTableStatementNode)
				BuildDropTable((DropTableStatementNode) node);
			if (node is DropViewStatementNode)
				BuildDropView((DropViewStatementNode) node);

			if (node is DeclareCursorNode)
				BuildDeclareCursor((DeclareCursorNode) node);
			if (node is OpenCursorStatementNode)
				BuildOpenCursor((OpenCursorStatementNode) node);
			if (node is CloseCursorStatementNode)
				BuildCloseCursor((CloseCursorStatementNode) node);
			if (node is FetchStatementNode)
				BuildFetchFromCursor((FetchStatementNode) node);

			if (node is CreateUserStatementNode)
				BuildCreateUser((CreateUserStatementNode) node);
			if (node is AlterUserStatementNode)
				BuildAlterUser((AlterUserStatementNode) node);
			if (node is GrantStatementNode)
				BuildGrant((GrantStatementNode) node);
			if (node is GrantRoleStatementNode)
				BuildGrant((GrantRoleStatementNode) node);

			if (node is ContinueStatementNode)
				BuildContinue((ContinueStatementNode) node);
			if (node is BreakStatementNode)
				BuildBreak((BreakStatementNode) node);
			if (node is ExitStatementNode)
				BuildExit((ExitStatementNode) node);

			if (node is RaiseStatementNode)
				BuildRaise((RaiseStatementNode) node);

			if (node is SequenceOfStatementsNode)
				BuildSequenceOfStatements((SequenceOfStatementsNode) node);
		}

		private void BuildGrant(GrantRoleStatementNode node) {
			foreach (var grantee in node.Grantees) {
				foreach (var role in node.Roles) {
					statements.Add(new GrantRoleStatement(grantee, role, node.WithAdmin));
				}
			}
		}

		private void BuildCreateSchema(CreateSchemaNode node) {
			statements.Add(new CreateSchemaStatement(node.SchemaName));
		}

		private void BuildGrant(GrantStatementNode node) {
			var objName = ObjectName.Parse(node.ObjectName);
			foreach (var grantee in node.Grantees) {
				foreach (var privilegeNode in node.Privileges) {
					var privilege = ParsePrivilege(privilegeNode.Privilege);
					statements.Add(new GrantPrivilegesStatement(grantee, privilege, node.WithGrant, objName, privilegeNode.Columns));
				}
			}
		}

		private static Privileges ParsePrivilege(string privName) {
			try {
				return (Privileges) Enum.Parse(typeof (Privileges), privName, true);
			} catch (Exception) {
				throw new InvalidOperationException(String.Format("Invalid privilege name '{0}' specified.", privName));
			}
		}
		

		private void BuildRaise(RaiseStatementNode node) {
			statements.Add(new RaiseStatement(node.ExceptionName));
		}

		private void BuildExit(ExitStatementNode node) {
			SqlExpression exp = null;
			if (node.WhenExpression != null)
				exp = ExpressionBuilder.Build(node.WhenExpression);

			statements.Add(new LoopControlStatement(LoopControlType.Exit, node.Label, exp));
		}

		private void BuildCreateSequence(CreateSequenceNode node) {
			var seqName = ObjectName.Parse(node.SequenceName);
			var statement = new CreateSequenceStatement(seqName);

			if (node.IncrementBy != null)
				statement.IncrementBy = Expression(node.IncrementBy);
			if (node.Cache != null)
				statement.Cache = Expression(node.Cache);
			if (node.StartWith != null)
				statement.StartWith = Expression(node.StartWith);
			if (node.MinValue != null)
				statement.MinValue = Expression(node.MinValue);
			if (node.MaxValue != null)
				statement.MaxValue = Expression(node.MaxValue);

			statement.Cycle = node.Cycle;

			statements.Add(statement);
		}

		private void BuildBreak(BreakStatementNode node) {
			SqlExpression exp = null;
			if (node.WhenExpression != null)
				exp = ExpressionBuilder.Build(node.WhenExpression);

			statements.Add(new LoopControlStatement(LoopControlType.Break, node.Label, exp));
		}

		private void BuildContinue(ContinueStatementNode node) {
			SqlExpression exp = null;
			if (node.WhenExpression != null)
				exp = ExpressionBuilder.Build(node.WhenExpression);

			statements.Add(new LoopControlStatement(LoopControlType.Continue, node.Label, exp));
		}

		private void BuildAlterUser(AlterUserStatementNode node) {
			AlterUser.Build(node, statements);
		}

		private void BuildCreateUser(CreateUserStatementNode node) {
			CreateUser.Build(node, statements);
		}

		private void BuildCloseCursor(CloseCursorStatementNode node) {
			statements.Add(new CloseStatement(node.CursorName));
		}

		private void BuildOpenCursor(OpenCursorStatementNode node) {
			var args = new List<SqlExpression>();
			if (node.Arguments != null) {
				args = node.Arguments.Select(ExpressionBuilder.Build).ToList();
			}

			statements.Add(new OpenStatement(node.CursorName, args.ToArray()));
		}

		private static bool TryParseDirection(string s, out FetchDirection direction) {
#if PCL
			return Enum.TryParse(s, true, out direction);
#else
			try {
				direction = (FetchDirection) Enum.Parse(typeof (FetchDirection), s, true);
				return true;
			} catch (Exception) {
				direction = new FetchDirection();
				return false;
			}
#endif
		}

		private void BuildFetchFromCursor(FetchStatementNode node) {
			FetchDirection direction;
			if (!TryParseDirection(node.Direction, out direction))
				throw new InvalidOperationException();

			var statement = new FetchStatement(node.CursorName, direction);
			if (node.Into != null)
				statement.IntoReference = Expression(node.Into);
			if (node.Position != null)
				statement.PositionExpression = Expression(node.Position);

			statements.Add(statement);
		}

		private SqlType BuildDataType(DataTypeNode node) {
			var builder = new DataTypeBuilder();
			return builder.Build(typeResolver, node);
		}

		private void BuildDeclareCursor(DeclareCursorNode node) {
			var parameters = new List<CursorParameter>();
			if (node.Parameters != null) {
				foreach (var parameterNode in node.Parameters) {
					var dataType = BuildDataType(parameterNode.ParameterType);
					parameters.Add(new CursorParameter(parameterNode.ParameterName, dataType));
				}
			}

			var flags = new CursorFlags();
			if (node.Insensitive)
				flags |= CursorFlags.Insensitive;
			if (node.Scroll)
				flags |= CursorFlags.Scroll;

			var queryExpression = (SqlQueryExpression) ExpressionBuilder.Build(node.QueryExpression);

			statements.Add(new DeclareCursorStatement(node.CursorName, parameters.ToArray(), flags, queryExpression));
		}

		private void BuildDropView(DropViewStatementNode node) {
			statements.Add(new DropViewStatement(node.ViewNames.ToArray(), node.IfExists));
		}

		private void BuildDropTable(DropTableStatementNode node) {
			statements.Add(new DropTableStatement(node.TableNames.ToArray(), node.IfExists));
		}

		private void BuildSequenceOfStatements(SequenceOfStatementsNode node) {
			foreach (var statementNode in node.Statements) {
				Build(statementNode);
			}
		}

		private void BuildSelect(SelectStatementNode node) {
			var queryExpression = (SqlQueryExpression) Expression(node.QueryExpression);
			if (node.QueryExpression.IntoClause != null) {
				var refExp = Expression(node.QueryExpression.IntoClause);
				statements.Add(new SelectIntoStatement(queryExpression, refExp));
			} else {
				var orderBy = OrderBy(node.OrderBy);
				var statement = new SelectStatement(queryExpression, orderBy);
				statement.Limit = Limit(node.Limit);
				statements.Add(statement);
			}
		}

		private IEnumerable<SortColumn> OrderBy(IEnumerable<OrderByNode> nodes) {
			if (nodes == null)
				return null;

			return nodes.Select(node => new SortColumn(Expression(node.Expression), node.Ascending));
		}

		private QueryLimit Limit(LimitNode node) {
			if (node == null)
				return null;

			return new QueryLimit(node.Offset, node.Count);
		}

		private void BuildCreateTrigger(CreateTriggerNode node) {
			throw new NotImplementedException();
		}

		private void BuildCreateView(CreateViewNode node) {
			var queryExpression = (SqlQueryExpression)Expression(node.QueryExpression);
			var statement = new CreateViewStatement(node.ViewName.Name, node.ColumnNames, queryExpression);
			statements.Add(statement);
		}

		private void BuildCreateTable(CreateTableNode node) {
			CreateTable.Build(typeResolver, node, statements);
		}

		private IEnumerable<SqlStatement> Build(ISqlNode rootNode, SqlQuery query) {
			Build(rootNode);
			return statements.ToArray();
		}

		private void BuildAlterTable(AlterTableNode node) {
			AlterTable.Build(typeResolver, node, statements);
		}

		public IEnumerable<SqlStatement> Build(ISqlNode rootNode, string query) {
			return Build(rootNode, new SqlQuery(query));
		}

		private void BuildInsert(InsertStatementNode node) {
			InsertIntoTable.Build(node, statements);
		}

		private void BuildUpdate(UpdateStatementNode node) {
			if (node.SimpleUpdate != null) {
				BuildSimpleUpdate(node.SimpleUpdate);
			} else if (node.QueryUpdate != null) {
				BuildQueryUpdate(node.QueryUpdate);
			}
		}

		private void BuildSimpleUpdate(SimpleUpdateNode node) {
			var whereExpression = Expression(node.WhereExpression);
			var assignments = UpdateAssignments(node.Columns);
			statements.Add(new UpdateStatement(node.TableName, whereExpression, assignments));
		}

		private IEnumerable<SqlColumnAssignment> UpdateAssignments(IEnumerable<UpdateColumnNode> columns) {
			if (columns == null)
				return null;

			return columns.Select(column => new SqlColumnAssignment(column.ColumnName, Expression(column.Expression)));
		}

		private void BuildQueryUpdate(QueryUpdateNode node) {
			throw new NotImplementedException();
		}

		private static SqlTableColumn BuildColumnInfo(ITypeResolver typeResolver, string tableName, TableColumnNode column, IList<SqlTableConstraint> constraints) {
			var dataTypeBuilder = new DataTypeBuilder();
			var dataType = dataTypeBuilder.Build(typeResolver, column.DataType);

			var columnInfo = new SqlTableColumn(column.ColumnName.Text, dataType);

			if (column.Default != null)
				columnInfo.DefaultExpression = ExpressionBuilder.Build(column.Default);

			if (column.IsIdentity) {
				columnInfo.DefaultExpression = SqlExpression.FunctionCall("UNIQUEKEY",
					new[] { SqlExpression.Constant(tableName) });
			}

			foreach (var constraint in column.Constraints) {
				if (String.Equals(ConstraintTypeNames.Check, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					var exp = ExpressionBuilder.Build(constraint.CheckExpression);
					constraints.Add(SqlTableConstraint.Check(null, exp));
				} else if (String.Equals(ConstraintTypeNames.ForeignKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					var fTable = constraint.ReferencedTable.Name;
					var fColumn = constraint.ReferencedColumn.Text;
					var onDelete = ForeignKeyAction.NoAction;
					var onUpdate = ForeignKeyAction.NoAction;
					
					if (!String.IsNullOrEmpty(constraint.OnDeleteAction))
						onDelete = GetForeignKeyAction(constraint.OnDeleteAction);
					if (!String.IsNullOrEmpty(constraint.OnUpdateAction))
						onUpdate = GetForeignKeyAction(constraint.OnUpdateAction);

					constraints.Add(SqlTableConstraint.ForeignKey(null, new[]{column.ColumnName.Text}, fTable, new[]{fColumn}, onDelete, onUpdate));
				} else if (String.Equals(ConstraintTypeNames.PrimaryKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					constraints.Add(SqlTableConstraint.PrimaryKey(null, new[]{column.ColumnName.Text}));
				} else if (String.Equals(ConstraintTypeNames.UniqueKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					constraints.Add(SqlTableConstraint.UniqueKey(null, new[]{column.ColumnName.Text}));
				} else if (String.Equals(ConstraintTypeNames.NotNull, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					columnInfo.IsNotNull = true;
				} else if (String.Equals(ConstraintTypeNames.Null, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
					columnInfo.IsNotNull = false;
				}
			}

			return columnInfo;
		}

		private static SqlTableConstraint BuildConstraint(TableConstraintNode constraint) {
			if (String.Equals(ConstraintTypeNames.Check, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
				var exp = ExpressionBuilder.Build(constraint.CheckExpression);
				return new SqlTableConstraint(constraint.ConstraintName, ConstraintType.Check, constraint.Columns.ToArray()) {
					CheckExpression = exp
				};
			}
			if (String.Equals(ConstraintTypeNames.PrimaryKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase))
				return SqlTableConstraint.PrimaryKey(constraint.ConstraintName, constraint.Columns.ToArray());
			if (String.Equals(ConstraintTypeNames.UniqueKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase))
				return SqlTableConstraint.UniqueKey(constraint.ConstraintName, constraint.Columns.ToArray());
			if (String.Equals(ConstraintTypeNames.ForeignKey, constraint.ConstraintType, StringComparison.OrdinalIgnoreCase)) {
				var fTable = constraint.ReferencedTableName.Name;
				var fColumns = constraint.ReferencedColumns;
				var onDelete = ForeignKeyAction.NoAction;
				var onUpdate = ForeignKeyAction.NoAction;

				if (!String.IsNullOrEmpty(constraint.OnDeleteAction))
					onDelete = GetForeignKeyAction(constraint.OnDeleteAction);
				if (!String.IsNullOrEmpty(constraint.OnUpdateAction))
					onUpdate = GetForeignKeyAction(constraint.OnUpdateAction);

				var fkey = SqlTableConstraint.ForeignKey(constraint.ConstraintName, constraint.Columns.ToArray(), fTable,
					fColumns.ToArray(), onDelete, onUpdate);

				return fkey;
			}

			throw new NotSupportedException();
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

#region CreateTable

		static class CreateTable {
			public static void Build(ITypeResolver typeResolver, CreateTableNode node, ICollection<SqlStatement> statements) {
				string idColumn = null;

				var tableName = node.TableName;
				var constraints = new List<SqlTableConstraint>();
				var columns = new List<SqlTableColumn>();

				foreach (var column in node.Columns) {
					if (column.IsIdentity) {
						if (!String.IsNullOrEmpty(idColumn))
							throw new InvalidOperationException(String.Format("Table {0} defines already {1} as identity column.",
								node.TableName, idColumn));

						if (column.Default != null)
							throw new InvalidOperationException(String.Format("The identity column {0} cannot have a DEFAULT constraint.",
								idColumn));

						idColumn = column.ColumnName.Text;
					}

					var columnInfo = BuildColumnInfo(typeResolver, tableName.Name, column, constraints);

					columns.Add(columnInfo);
				}

				foreach (var constraint in node.Constraints) {
					var constraintInfo = BuildConstraint(constraint);
					statements.Add(new AlterTableStatement(ObjectName.Parse(tableName.Name), new AddConstraintAction(constraintInfo)));
				}

				//TODO: Optimization: merge same constraints

				statements.Add(MakeCreateTable(tableName.Name, columns, node.IfNotExists, node.Temporary));

				foreach (var constraint in constraints) {
					statements.Add(MakeAlterTableAddConstraint(tableName.Name, constraint));
				}
			}

			private static SqlStatement MakeAlterTableAddConstraint(string tableName, SqlTableConstraint constraint) {
				var action = new AddConstraintAction(constraint);

				return new AlterTableStatement(ObjectName.Parse(tableName), action);
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

		static class AlterTable {
			public static void Build(ITypeResolver typeResolver, AlterTableNode node, ICollection<SqlStatement> statements) {
				if (node.CreateTable != null) {
					CreateTable.Build(typeResolver, node.CreateTable, statements);
					foreach (var statement in statements) {
						if (statement is CreateTableStatement)
							((CreateTableStatement) statement).IfNotExists = true;
					}
				} else if (node.Actions != null) {
					foreach (var action in node.Actions) {
						BuildAction(typeResolver, ObjectName.Parse(node.TableName), action, statements);
					}
				}
			}

			private static void BuildAction(ITypeResolver typeResolver, ObjectName tableName, IAlterActionNode action, ICollection<SqlStatement> statements) {
				if (action is AddColumnNode) {
					var column = ((AddColumnNode) action).Column;
					var constraints = new List<SqlTableConstraint>();
					var columnInfo = BuildColumnInfo(typeResolver, tableName.FullName, column, constraints);

					statements.Add(new AlterTableStatement(tableName, new AddColumnAction(columnInfo)));

					foreach (var constraint in constraints) {
						statements.Add(new AlterTableStatement(tableName, new AddConstraintAction(constraint)));
					}
				} else if (action is AddConstraintNode) {
					var constraint = ((AddConstraintNode) action).Constraint;

					var constraintInfo = BuildConstraint(constraint);
					statements.Add(new AlterTableStatement(tableName, new AddConstraintAction(constraintInfo)));
				} else if (action is DropColumnNode) {
					var columnName = ((DropColumnNode) action).ColumnName;
					statements.Add(new AlterTableStatement(tableName, new DropColumnAction(columnName)));
				} else if (action is DropConstraintNode) {
					var constraintName = ((DropConstraintNode) action).ConstraintName;
					statements.Add(new AlterTableStatement(tableName, new DropConstraintAction(constraintName)));
				} else if (action is SetDefaultNode) {
					var actionNode = ((SetDefaultNode) action);
					var columnName =actionNode.ColumnName;
					var expression = ExpressionBuilder.Build(actionNode.Expression);
					statements.Add(new AlterTableStatement(tableName, new SetDefaultAction(columnName, expression)));
				} else if (action is DropDefaultNode) {
					var columnName = ((DropDefaultNode) action).ColumnName;
					statements.Add(new AlterTableStatement(tableName, new DropDefaultAction(columnName)));
				} else if (action is AlterColumnNode) {
					var column = ((AlterColumnNode) action).Column;
					var constraints = new List<SqlTableConstraint>();
					var columnInfo = BuildColumnInfo(typeResolver, tableName.FullName, column, constraints);

					// CHECK: Here we do a drop and add column: is there a better way on the back-end?
					statements.Add(new AlterTableStatement(tableName, new DropColumnAction(columnInfo.ColumnName)));

					statements.Add(new AlterTableStatement(tableName, new AddColumnAction(columnInfo)));

					foreach (var constraint in constraints) {
						statements.Add(new AlterTableStatement(tableName, new AddConstraintAction(constraint)));
					}
				}
			}
		}

#endregion

#region InsertIntoTable

		static class InsertIntoTable {
			public static void Build(InsertStatementNode node, ICollection<SqlStatement> statements) {
				if (node.ValuesInsert != null) {
					var valueInsert = node.ValuesInsert;
					var values =
						valueInsert.Values.Select(setNode => setNode.Values.Select(ExpressionBuilder.Build).ToArray()).ToList();
					statements.Add(new InsertStatement(node.TableName, node.ColumnNames, values));
				} else if (node.SetInsert != null) {
					var assignments = node.SetInsert.Assignments;

					var columnNames = new List<string>();
					var values = new List<SqlExpression>();
					foreach (var assignment in assignments) {
						var columnName = assignment.ColumnName;
						var value = ExpressionBuilder.Build(assignment.Value);

						columnNames.Add(columnName);
						values.Add(value);
					}

					statements.Add(new InsertStatement(node.TableName, columnNames.ToArray(), new[] {values.ToArray()}));
				} else if (node.QueryInsert != null) {
					var queryInsert = node.QueryInsert;
					var queryExpression = ExpressionBuilder.Build(queryInsert.QueryExpression) as SqlQueryExpression;
					if (queryExpression == null)
						throw new SqlParseException();

					statements.Add(new InsertSelectStatement(node.TableName, node.ColumnNames, queryExpression));
				}
			}
		}

#endregion

#region CreateUser

		static class CreateUser {
			public static void Build(CreateUserStatementNode node, ICollection<SqlStatement> statements) {
				if (node.Identificator is IdentifiedByPasswordNode) {
					var passwordNode = (IdentifiedByPasswordNode) node.Identificator;
					var password = ExpressionBuilder.Build(passwordNode.Password);
					statements.Add(new CreateUserStatement(node.UserName, password));
				} else {
					throw new NotSupportedException();
				}
			}
		}

#endregion

#region AlterUser

		static class AlterUser {
			public static void Build(AlterUserStatementNode node, ICollection<SqlStatement> statements) {
				throw new NotImplementedException();
			}
		}

#endregion
	}
}
