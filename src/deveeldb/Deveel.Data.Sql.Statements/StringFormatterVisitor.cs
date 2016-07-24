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

		protected override SqlStatement VisitDeclareException(DeclareExceptionStatement statement) {
			builder.AppendFormat("EXCEPTION {0}", statement.ExceptionName);

			return base.VisitDeclareException(statement);
		}

		protected override SqlStatement VisitSelectInto(SelectIntoStatement statement) {
			// TODO: here we need to decompose the query and inject the INTO clause
			return base.VisitSelectInto(statement);
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
			(statement as ISqlFormattable).AppendTo(builder);
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
			(statement as ISqlFormattable).AppendTo(builder);
			return base.VisitDropFunction(statement);
		}

		protected override SqlStatement VisitDropCallbackTrigger(DropCallbackTriggersStatement statement) {
			builder.AppendFormat("DROP CALLBACK TRIGGER {0}", statement.TriggerName);

			return base.VisitDropCallbackTrigger(statement);
		}

		protected override SqlStatement VisitDropProcedure(DropProcedureStatement statement) {

			return base.VisitDropProcedure(statement);
		}

		protected override SqlStatement VisitDropRole(DropRoleStatement statement) {
			builder.AppendFormat("DROP ROLE {0}", statement.RoleName);

			return base.VisitDropRole(statement);
		}

		protected override SqlStatement VisitDropSequence(DropSequenceStatement statement) {

			return base.VisitDropSequence(statement);
		}

		protected override SqlStatement VisitDropTable(DropTableStatement statement) {
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

			return base.VisitDropView(statement);
		}

		protected override SqlStatement VisitDropType(DropTypeStatement statement) {
			return base.VisitDropType(statement);
		}

		protected override SqlStatement VisitGrantPrivilege(GrantPrivilegesStatement statement) {

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