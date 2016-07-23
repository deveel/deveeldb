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
using System.Linq;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	class StringFormatterVisitor : StatementVisitor {
		private readonly SqlStringBuilder builder;

		public StringFormatterVisitor() {
			builder = new SqlStringBuilder();
		}

		public string Format(SqlStatement statement) {
			VisitStatement(statement);
			return builder.ToString();
		}

		protected override SqlStatement VisitCall(CallStatement statement) {
			builder.AppendFormat("CALL {0}(", statement.ProcedureName);
			if (statement.Arguments != null &&
				statement.Arguments.Length > 0) {
				for (int i = 0; i < statement.Arguments.Length; i++) {
					builder.Append(statement.Arguments[i]);

					if (i < statement.Arguments.Length -1)
						builder.Append(", ");
				}
			}

			builder.Append(")");

			return base.VisitCall(statement);
		}

		protected override SqlStatement VisitAlterTable(AlterTableStatement statement) {
			builder.AppendFormat("ALTER TABLE {0} ", statement.TableName);

			if (statement.Action is AddColumnAction) {
				var columnAdd = (AddColumnAction) statement.Action;
				builder.AppendFormat("ADD COLUMN {0} {1}", columnAdd.Column.ColumnName, columnAdd.Column.ColumnType);
				if (columnAdd.Column.IsNotNull)
					builder.Append(" NOT NULL");
				if (columnAdd.Column.HasDefaultExpression)
					builder.AppendFormat(" DEFAULT {0}", columnAdd.Column.DefaultExpression);
			} else if (statement.Action is AddConstraintAction) {
				var addConstraint = (AddConstraintAction) statement.Action;
				string constraintType;
				if (addConstraint.Constraint.ConstraintType == ConstraintType.ForeignKey) {
					constraintType = "FOREIGN KEY";
				} else if (addConstraint.Constraint.ConstraintType == ConstraintType.PrimaryKey) {
					constraintType = "PRIMARY KEY";
				} else {
					constraintType = addConstraint.Constraint.ConstraintType.ToString().ToUpperInvariant();
				}

				builder.Append("ADD CONSTRAINT ");
				if (!String.IsNullOrEmpty(addConstraint.Constraint.ConstraintName)) {
					builder.Append(addConstraint.Constraint.ConstraintName);
					builder.Append(" ");
				}

				builder.Append(constraintType);

				if (addConstraint.Constraint.Columns != null &&
					addConstraint.Constraint.Columns.Length > 0)
					builder.AppendFormat("({0})", String.Join(", ", addConstraint.Constraint.Columns));

				if (addConstraint.Constraint.ConstraintType == ConstraintType.ForeignKey) {
					string onDelete = addConstraint.Constraint.OnDelete.AsSqlString();
					string onUpdate = addConstraint.Constraint.OnUpdate.AsSqlString();

					builder.AppendFormat(" REFERENCES {0}({1}) ON DELETE {2} ON UPDATE {3}", addConstraint.Constraint.ReferenceTable,
						String.Join(", ", addConstraint.Constraint.ReferenceColumns), onDelete, onUpdate);
				} else if (addConstraint.Constraint.ConstraintType == ConstraintType.Check) {
					builder.AppendFormat(" {0}", addConstraint.Constraint.CheckExpression);
				}
			} else if (statement.Action is DropColumnAction) {
				var dropColumn = (DropColumnAction) statement.Action;
				builder.AppendFormat("DROP COLUMN {0}", dropColumn.ColumnName);
			} else if (statement.Action is DropConstraintAction) {
				var dropConstraint = (DropConstraintAction) statement.Action;
				builder.AppendFormat("DROP CONSTRAINT {0}", dropConstraint.ConstraintName);
			} else if (statement.Action is DropDefaultAction) {
				var dropDefault = (DropDefaultAction) statement.Action;
				builder.AppendFormat("DROP DEFAULT {0}", dropDefault.ColumnName);
			} else if (statement.Action is DropPrimaryKeyAction) {
				builder.Append("DROP PRIMARY KEY");
			}

			return base.VisitAlterTable(statement);
		}

		protected override SqlStatement VisitLoop(LoopStatement statement) {
			if (!String.IsNullOrEmpty(statement.Label)) {
				builder.AppendFormat("<<{0}>>", statement.Label);
				builder.AppendLine();
			}

			if (statement is WhileLoopStatement) {
				var whileLoop = (WhileLoopStatement) statement;

				builder.AppendFormat("WHILE {0}", whileLoop.ConditionExpression);
				builder.AppendLine();
			} else if (statement is ForLoopStatement) {
				var forLoop = (ForLoopStatement) statement;
				builder.AppendFormat("FOR {0} ", forLoop.IndexName);

				if (forLoop.Reverse)
					builder.Append("REVERSE");

				builder.AppendFormat("IN {0}...{1}", forLoop.LowerBound, forLoop.UpperBound);
				builder.AppendLine();
			} else if (statement is CursorForLoopStatement) {
				var forLoop = (CursorForLoopStatement) statement;

				builder.AppendFormat("FOR {0} IN {1}", forLoop.IndexName, forLoop.CursorName);
				builder.AppendLine();
			}

			builder.AppendLine("LOOP");
			builder.Indent();

			foreach (var child in statement.Statements) {
				VisitStatement(child);
				builder.AppendLine();
			}

			builder.DeIndent();
			builder.Append("END LOOP");

			return statement;
		}

		protected override SqlStatement VisitCondition(ConditionStatement statement) {
			builder.Append("IF ");
			builder.Append(statement.ConditionExpression);
			builder.Append(" THEN");
			builder.AppendLine();

			builder.Indent();

			foreach (var child in statement.TrueStatements) {
				VisitStatement(child);
				builder.AppendLine();
			}

			if (statement.FalseStatements != null &&
				statement.FalseStatements.Length > 0) {
				builder.DeIndent();

				builder.AppendLine("ELSE");
				builder.Indent();

				foreach (var child in statement.FalseStatements) {
					VisitStatement(child);
					builder.AppendLine();
				}
			}

			builder.DeIndent();
			builder.Append("END IF");

			return statement;
		}

		protected override SqlStatement VisitGoTo(GoToStatement statement) {
			builder.AppendFormat("GOTO '{0}'", statement.Label);

			return base.VisitGoTo(statement);
		}

		protected override SqlStatement VisitRaise(RaiseStatement statement) {
			builder.AppendFormat("RAISE {0}", statement.ExceptionName);
			return base.VisitRaise(statement);
		}

		protected override SqlStatement VisitLoopControl(LoopControlStatement statement) {
			var type = statement.ControlType.ToString().ToUpperInvariant();
			builder.Append(type);

			if (!String.IsNullOrEmpty(statement.Label))
				builder.AppendFormat(" '{0}'", statement.Label);

			if (statement.WhenExpression != null) {
				builder.AppendFormat(" WHEN {0}", statement.WhenExpression);
			}

			return base.VisitLoopControl(statement);
		}

		private void VisitExceptionHandler(ExceptionHandler handler) {
			builder.Append("WHEN ");
			if (handler.Handled.IsForOthers) {
				builder.Append("OTHERS");
			} else {
				var names = String.Join(", ", handler.Handled.ExceptionNames.ToArray());
				builder.Append(names);
			}

			builder.AppendLine("THEN ");
			builder.Indent();

			foreach (var statement in handler.Statements) {
				VisitStatement(statement);
				builder.AppendLine();
			}

			builder.DeIndent();
		}

		protected override SqlStatement VisitPlSqlBlock(PlSqlBlockStatement statement) {
			if (!String.IsNullOrEmpty(statement.Label)) {
				builder.AppendFormat("<<{0}>>", statement.Label);
				builder.AppendLine();
			}

			if (statement.Declarations != null &&
				statement.Declarations.Count > 0) {
				builder.AppendLine("DECLARE");
				builder.Indent();

				foreach (var declaration in statement.Declarations) {
					VisitStatement(declaration);
					builder.AppendLine();
				}

				builder.DeIndent();
			}

			builder.AppendLine("BEGIN");
			builder.Indent();

			foreach (var child in statement.Statements) {
				VisitStatement(child);
				builder.AppendLine();
			}

			builder.DeIndent();

			if (statement.ExceptionHandlers != null &&
				statement.ExceptionHandlers.Count > 0) {
				builder.AppendLine("EXCEPTION");
				builder.Indent();

				foreach (var handler in statement.ExceptionHandlers) {
					VisitExceptionHandler(handler);
					builder.AppendLine();
				}

				builder.DeIndent();
			}

			builder.Append("END");

			return statement;
		}

		protected override SqlStatement VisitDeclareCursor(DeclareCursorStatement statement) {
			// TODO: Flags ...

			builder.AppendFormat("CURSOR {0}", statement.CursorName);

			if (statement.Parameters != null) {
				var pars = statement.Parameters.ToArray();

				builder.Append(" (");

				for (int i = 0; i < pars.Length; i++) {
					var p = pars[i];

					builder.Append(p);

					if (i < pars.Length - 1)
						builder.Append(", ");
				}

				builder.Append(")");
			}

			builder.AppendLine();
			builder.Indent();

			builder.Append(" IS ");
			builder.Append(statement.QueryExpression);

			builder.DeIndent();

			return base.VisitDeclareCursor(statement);
		}

		protected override SqlStatement VisitDeclareException(DeclareExceptionStatement statement) {
			builder.AppendFormat("EXCEPTION {0}", statement.ExceptionName);

			return base.VisitDeclareException(statement);
		}

		protected override SqlStatement VisitDeclareVariable(DeclareVariableStatement statement) {
			if (statement.IsConstant)
				builder.Append("CONSTANT ");

			builder.AppendFormat("{0} {1}", statement.VariableName, statement.VariableType);

			if (statement.IsNotNull)
				builder.Append(" NOT NULL");

			if (statement.DefaultExpression != null) {
				builder.Append(" := ");
				builder.Append(statement.DefaultExpression);
			}

			return base.VisitDeclareVariable(statement);
		}

		protected override SqlStatement VisitSelect(SelectStatement statement) {
			builder.Append(statement.QueryExpression);

			if (statement.Limit != null) {
				builder.AppendLine();
				builder.Indent();
				builder.Append(" LIMIT ");

				if (statement.Limit.Offset > -1)
					builder.AppendFormat("{0}, ", statement.Limit.Offset);

				builder.Append(statement.Limit.Count);
				builder.DeIndent();
			}

			if (statement.OrderBy != null) {
				builder.AppendLine();
				builder.Indent();

				builder.Append("ORDER BY ");

				var orderBy = statement.OrderBy.ToArray();
				for (int i = 0; i < orderBy.Length; i++) {
					builder.Append(orderBy[i].Expression);

					if (orderBy[i].Ascending) {
						builder.Append(" ASC");
					} else {
						builder.Append(" DESC");
					}

					if (i < orderBy.Length - 1)
						builder.Append(", ");
				}

				builder.DeIndent();
			}

			return base.VisitSelect(statement);
		}

		protected override SqlStatement VisitSelectInto(SelectIntoStatement statement) {
			// TODO: here we need to decompose the query and inject the INTO clause
			return base.VisitSelectInto(statement);
		}

		protected override SqlStatement VisitCreateSchema(CreateSchemaStatement statement) {
			builder.AppendFormat("CREATE SCHEMA {0}", statement.SchemaName);
			return base.VisitCreateSchema(statement);
		}

		protected override SqlStatement VisitCommit(CommitStatement statement) {
			builder.Append("COMMIT");
			return base.VisitCommit(statement);
		}

		protected override SqlStatement VisitRollback(RollbackStatement statement) {
			builder.Append("ROLLBACK");
			return base.VisitRollback(statement);
		}

		protected override SqlStatement VisitOpen(OpenStatement statement) {
			builder.AppendFormat("OPEN {0}", statement.CursorName);

			if (statement.Arguments != null && statement.Arguments.Length > 0) {
				builder.Append("(");
				for (int i = 0; i < statement.Arguments.Length; i++) {
					builder.Append(statement.Arguments[i]);

					if (i < statement.Arguments.Length - 1)
						builder.Append(", ");
				}

				builder.Append(")");
			}

			return base.VisitOpen(statement);
		}

		protected override SqlStatement VisitClose(CloseStatement statement) {
			builder.AppendFormat("CLOSE {0}", statement.CursorName);
			return base.VisitClose(statement);
		}

		protected override SqlStatement VisitAssignVariable(AssignVariableStatement statement) {
			builder.AppendFormat("{0} := {1}", statement.VariableReference, statement.ValueExpression);

			return base.VisitAssignVariable(statement);
		}

		protected override SqlStatement VisitFetch(FetchStatement statement) {
			builder.AppendFormat("FETCH {0}", statement.Direction.ToString().ToUpperInvariant());

			if (statement.OffsetExpression != null)
				builder.AppendFormat(" {0}", statement.OffsetExpression);

			if (!String.IsNullOrEmpty(statement.CursorName))
				builder.AppendFormat(" FROM {0}", statement.CursorName);

			return base.VisitFetch(statement);
		}

		protected override SqlStatement VisitReturn(ReturnStatement statement) {
			builder.Append("RETURN");
			if (statement.ReturnExpression != null)
				builder.AppendFormat(" {0}", statement.ReturnExpression);

			return base.VisitReturn(statement);
		}

		protected override SqlStatement VisitSet(SetStatement statement) {
			builder.AppendFormat("SET {0} {1}", statement.SettingName, statement.ValueExpression);

			return base.VisitSet(statement);
		}

		protected override SqlStatement VisitDelete(DeleteStatement statement) {
			builder.AppendFormat("DELETE FROM {0} WHERE {1}", statement.TableName, statement.WhereExpression);
			if (statement.Limit > -1)
				builder.AppendFormat(" LIMIT {0}", statement.Limit);

			return base.VisitDelete(statement);
		}

		protected override SqlStatement VisitAlterUser(AlterUserStatement statement) {
			builder.AppendFormat("ALTER USER {0} ", statement.UserName);

			if (statement.AlterAction is SetUserRolesAction) {
				var setRoles = (SetUserRolesAction) statement.AlterAction;

				var roles = String.Join(", ", setRoles.Roles.Select(x => x.ToString()).ToArray());
				builder.AppendFormat(" SET ROLE ", roles);
			} else if (statement.AlterAction is SetPasswordAction) {
				var setPassword = (SetPasswordAction) statement.AlterAction;
				builder.AppendFormat(" SET PASSWORD {0}", setPassword.PasswordExpression);
			} else if (statement.AlterAction is SetAccountStatusAction) {
				var setStatus = (SetAccountStatusAction) statement.AlterAction;
				builder.AppendFormat(" SET ACCOUNT STATUS {0}", setStatus.ActionType.ToString().ToUpperInvariant());
			}

			return base.VisitAlterUser(statement);
		}

		protected override SqlStatement VisitCreateCallbackTrigger(CreateCallbackTriggerStatement statement) {
			builder.AppendFormat("CREATE CALLBACK TRIGGER {0} ON {1}", statement.TriggerName, statement.TableName);

			// TODO: Continue
			return base.VisitCreateCallbackTrigger(statement);
		}

		private void VisitRoutineParameters(IEnumerable<RoutineParameter> parameters) {
			if (parameters != null) {
				var array = parameters.ToArray();

				builder.Append("(");

				for (int i = 0; i < array.Length; i++) {
					VisitRoutineParameter(array[i]);

					if (i < array.Length -1)
						builder.Append(", ");
				}

				builder.Append(") ");
			}
		}

		private void VisitRoutineParameter(RoutineParameter parameter) {
			builder.AppendFormat("{0} ", parameter.Name);

			if (parameter.IsInput)
				builder.Append(" IN");
			if (parameter.IsOutput)
				builder.Append(" OUT");

			builder.Append(parameter.Type);

			if (!parameter.IsNullable)
				builder.Append(" NOT NULL");

			if (parameter.IsUnbounded) {
				builder.Append(" UNBOUNDED");
			}

			// TODO: Default value for the argument
		}

		protected override SqlStatement VisitCreateExternFunction(CreateExternalFunctionStatement statement) {
			var orReplace = statement.ReplaceIfExists ? "OR REPLACE" : "";
			builder.AppendFormat("CREATE {0} FUNCTION {1}", orReplace, statement.FunctionName);

			VisitRoutineParameters(statement.Parameters);

			builder.AppendFormat("RETURNS {0} IS", statement.ReturnType);
			builder.AppendLine();
			builder.Indent();
			builder.AppendFormat("LANGUAGE DOTNET NAME '{0}'", statement.ExternalReference);
			builder.DeIndent();

			return base.VisitCreateExternFunction(statement);
		}

		protected override SqlStatement VisitCreateFunction(CreateFunctionStatement statement) {
			var orReplace = statement.ReplaceIfExists ? "OR REPLACE" : "";
			builder.AppendFormat("CREATE {0} FUNCTION {1}", orReplace, statement.FunctionName);

			VisitRoutineParameters(statement.Parameters);

			builder.AppendFormat("RETURNS {0} IS", statement.ReturnType);
			builder.AppendLine();
			builder.Indent();

			VisitStatement(statement.Body);

			builder.DeIndent();

			return base.VisitCreateFunction(statement);
		}

		protected override SqlStatement VisitCreateRole(CreateRoleStatement statement) {
			builder.AppendFormat("CREATE ROLE {0}", statement.RoleName);

			return base.VisitCreateRole(statement);
		}

		protected override SqlStatement VisitCreateProcedure(CreateProcedureStatement statement) {
			var orReplace = statement.ReplaceIfExists ? "OR REPLACE" : "";
			builder.AppendFormat("CREATE {0} PROCEDURE {1}", orReplace, statement.ProcedureName);

			VisitRoutineParameters(statement.Parameters);

			builder.Append("IS");

			builder.AppendLine();
			builder.Indent();

			VisitStatement(statement.Body);

			builder.DeIndent();

			return base.VisitCreateProcedure(statement);
		}

		protected override SqlStatement VisitCreateExternProcedure(CreateExternalProcedureStatement statement) {
			var orReplace = statement.ReplaceIfExists ? "OR REPLACE" : "";
			builder.AppendFormat("CREATE {0} PROCEDURE {1}", orReplace, statement.ProcedureName);

			VisitRoutineParameters(statement.Parameters);

			builder.Append("IS");
			builder.AppendLine();
			builder.Indent();
			builder.AppendFormat("LANGUAGE DOTNET NAME '{0}'", statement.ExternalReference);
			builder.DeIndent();

			return base.VisitCreateExternProcedure(statement);
		}

		protected override SqlStatement VisitCreateUser(CreateUserStatement statement) {
			builder.AppendFormat("CREATE USER {0}", statement.UserName);

			if (statement.Password != null)
				builder.AppendFormat(" IDENTIFIED BY PASSWORD {0}", statement.Password);

			return base.VisitCreateUser(statement);
		}

		protected override SqlStatement VisitCreateSequence(CreateSequenceStatement statement) {
			builder.AppendFormat("CREATE SEQUENCE {0}", statement.SequenceName);

			if (statement.StartWith != null)
				builder.AppendFormat(" START WITH {0}", statement.StartWith);
			if (statement.MinValue != null)
				builder.AppendFormat(" MIN VALUE {0}", statement.MinValue);
			if (statement.MaxValue != null)
				builder.AppendFormat(" MAX VALUE {0}", statement.MaxValue);
			if (statement.IncrementBy != null)
				builder.AppendFormat(" INCREMENT BY {0}", statement.IncrementBy);
			if (statement.Cache != null)
				builder.AppendFormat(" CACHE {0}", statement.Cache);
			if (statement.Cycle)
				builder.Append(" CYCLE");

			return base.VisitCreateSequence(statement);
		}

		protected override SqlStatement VisitCreateProcedureTrigger(CreateProcedureTriggerStatement statement) {
			return base.VisitCreateProcedureTrigger(statement);
		}

		protected override SqlStatement VisitCreateTable(CreateTableStatement statement) {
			builder.Append("CREATE TABLE ");
			if (statement.IfNotExists)
				builder.Append("IF NOT EXISTS ");

			builder.Append(statement.TableName);
			builder.AppendLine(" (");
			builder.Indent();

			for (int i = 0; i < statement.Columns.Count; i++) {
				var column = statement.Columns[i];

				builder.AppendFormat("{0} {1}", column.ColumnName, column.ColumnType.ToString());

				if (column.IsIdentity) {
					builder.Append(" IDENTITY");
				} else {
					if (column.IsNotNull)
						builder.Append(" NOT NULL");
					if (column.HasDefaultExpression)
						builder.AppendFormat(" DEFAULT {0}", column.DefaultExpression);
				}

				if (i < statement.Columns.Count - 1)
					builder.Append(",");

				builder.AppendLine();
			}

			builder.DeIndent();
			builder.Append(")");

			return base.VisitCreateTable(statement);
		}

		protected override SqlStatement VisitCreateTrigger(CreateTriggerStatement statement) {
			return base.VisitCreateTrigger(statement);
		}

		protected override SqlStatement VisitCreateView(CreateViewStatement statement) {
			string ifNotExists = statement.ReplaceIfExists ? "IF NOT EXISTS " : "";
			builder.AppendFormat("CREATE {0}VIEW {1}", ifNotExists, statement.ViewName);

			if (statement.ColumnNames != null) {
				var colNames = String.Join(", ", statement.ColumnNames.ToArray());
				builder.AppendFormat(" ({0})", colNames);
			}

			builder.Append(" IS");
			builder.AppendLine();
			builder.Indent();
			builder.Append(statement.QueryExpression);
			builder.DeIndent();

			return statement;
		}

		protected override SqlStatement VisitDropFunction(DropFunctionStatement statement) {
			string ifExists = statement.IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP FUNCTION {0}{1}", ifExists, statement.FunctionName);

			return base.VisitDropFunction(statement);
		}

		protected override SqlStatement VisitDropCallbackTrigger(DropCallbackTriggersStatement statement) {
			builder.AppendFormat("DROP CALLBACK TRIGGER {0}", statement.TriggerName);

			return base.VisitDropCallbackTrigger(statement);
		}

		protected override SqlStatement VisitDropProcedure(DropProcedureStatement statement) {
			string ifExists = statement.IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP PROCEDURE {0}{1}", ifExists, statement.ProcedureName);

			return base.VisitDropProcedure(statement);
		}

		protected override SqlStatement VisitDropRole(DropRoleStatement statement) {
			builder.AppendFormat("DROP ROLE {0}", statement.RoleName);

			return base.VisitDropRole(statement);
		}

		protected override SqlStatement VisitDropSequence(DropSequenceStatement statement) {
			builder.AppendFormat("DROP SEQUENCE {0}", statement.SequenceName);

			return base.VisitDropSequence(statement);
		}

		protected override SqlStatement VisitDropTable(DropTableStatement statement) {
			string ifExists = statement.IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP TABLE {0}{1}", ifExists, statement.TableName);

			return base.VisitDropTable(statement);
		}

		protected override SqlStatement VisitDropTrigger(DropTriggerStatement statement) {
			builder.AppendFormat("DROP TRIGGER {0}", statement.TriggerName);

			return base.VisitDropTrigger(statement);
		}

		protected override SqlStatement VisitDropUser(DropUserStatement statement) {
			builder.AppendFormat("DROP USER {0}", statement.UserName);

			return base.VisitDropUser(statement);
		}

		protected override SqlStatement VisitDropView(DropViewStatement statement) {
			string ifExists = statement.IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP VIEW {0}{1}", ifExists, statement.ViewName);

			return base.VisitDropView(statement);
		}

		protected override SqlStatement VisitDropType(DropTypeStatement statement) {
			string ifExists = statement.IfExists ? "IF EXISTS " : "";
			builder.AppendFormat("DROP TYPE {0}{1}", ifExists, statement.TypeName);

			return base.VisitDropType(statement);
		}

		protected override SqlStatement VisitGrantPrivilege(GrantPrivilegesStatement statement) {
			// TODO: Make it SQL string
			var privs = statement.Privilege.ToString().ToUpperInvariant();
			builder.AppendFormat("GRANT {0} TO {1} ON {2}", privs, statement.Grantee, statement.ObjectName);

			if (statement.Columns != null) {
				var columns = statement.Columns.ToArray();
				if (columns.Length > 0) {
					builder.AppendFormat("({0})", String.Join(", ", columns));
				}
			}

			if (statement.WithGrant)
				builder.Append(" WITH GRANT OPTION");

			return base.VisitGrantPrivilege(statement);
		}

		protected override SqlStatement VisitRevokePrivilege(RevokePrivilegesStatement statement) {
			return base.VisitRevokePrivilege(statement);
		}

		protected override SqlStatement VisitGrantRole(GrantRoleStatement statement) {
			return base.VisitGrantRole(statement);
		}

		protected override SqlStatement VisitShow(ShowStatement statement) {
			builder.AppendFormat("SHOW {0}", statement.Target.ToString().ToUpperInvariant());

			if (statement.TableName != null)
				builder.AppendFormat(" FROM {0}", statement.TableName);

			return base.VisitShow(statement);
		}

		protected override SqlStatement VisitInsert(InsertStatement statement) {
			builder.Append("INSERT INTO ");
			builder.Append(statement.TableName);

			if (statement.ColumnNames != null &&
			    statement.ColumnNames.Length > 0) {
				builder.Append("(");
				builder.Append(String.Join(", ", statement.ColumnNames));
				builder.Append(")");
			}

			var values = statement.Values.ToList();
			if (values.Count > 1) {
				builder.AppendLine();

				builder.Indent();
				builder.AppendLine("VALUES");

				for (int i = 0; i < values.Count; i++) {
					var set = values[i];

					builder.Append("(");

					for (int j = 0; j < set.Length; j++) {
						builder.Append(set[j]);

						if (j < set.Length - 1)
							builder.Append(", ");
					}

					builder.Append(")");

					if (i < values.Count - 1) {
						builder.AppendLine(",");
					}
				}
			} else {
				builder.Append(" VALUES ");
				var set = values[0];

				builder.Append("(");
				for (int i = 0; i < set.Length; i++) {
					builder.Append(set[i]);

					if (i < set.Length -1)
						builder.Append(", ");
				}
				builder.Append(")");
			}

			return statement;
		}

		protected override SqlStatement VisitInsertSelect(InsertSelectStatement statement) {
			return base.VisitInsertSelect(statement);
		}

		protected override SqlStatement VisitUpdate(UpdateStatement statement) {
			return base.VisitUpdate(statement);
		}

		protected override SqlStatement VisitUpdateFromCursor(UpdateFromCursorStatement statement) {
			return base.VisitUpdateFromCursor(statement);
		}
	}
}