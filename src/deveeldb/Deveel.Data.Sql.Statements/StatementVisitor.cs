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

namespace Deveel.Data.Sql.Statements {
	public class StatementVisitor {
		protected virtual SqlStatement VisitStatement(SqlStatement statement) {
			if (statement == null)
				return null;

			// CREATE
			if (statement is CreateTableStatement)
				return VisitCreateTable((CreateTableStatement) statement);
			if (statement is CreateViewStatement)
				return VisitCreateView((CreateViewStatement) statement);
			if (statement is CreateSchemaStatement)
				return VisitCreateSchema((CreateSchemaStatement) statement);
			if (statement is CreateSequenceStatement)
				return VisitCreateSequence((CreateSequenceStatement) statement);
			if (statement is CreateTriggerStatement)
				return VisitCreateTrigger((CreateTriggerStatement) statement);
			if (statement is CreateCallbackTriggerStatement)
				return VisitCreateCallbackTrigger((CreateCallbackTriggerStatement) statement);
			if (statement is CreateProcedureTriggerStatement)
				return VisitCreateProcedureTrigger((CreateProcedureTriggerStatement) statement);
			if (statement is CreateUserStatement)
				return VisitCreateUser((CreateUserStatement) statement);
			if (statement is CreateRoleStatement)
				return VisitCreateRole((CreateRoleStatement) statement);
			if (statement is CreateProcedureStatement)
				return VisitCreateProcedure((CreateProcedureStatement) statement);
			if (statement is CreateExternalProcedureStatement)
				return VisitCreateExternProcedure((CreateExternalProcedureStatement) statement);
			if (statement is CreateFunctionStatement)
				return VisitCreateFunction((CreateFunctionStatement) statement);
			if (statement is CreateExternalFunctionStatement)
				return VisitCreateExternFunction((CreateExternalFunctionStatement) statement);
			
			// DROP
			if (statement is DropViewStatement)
				return VisitDropView((DropViewStatement) statement);
			if (statement is DropTableStatement)
				return VisitDropTable((DropTableStatement) statement);
			if (statement is DropCallbackTriggersStatement)
				return VisitDropCallbackTrigger((DropCallbackTriggersStatement) statement);
			if (statement is DropSequenceStatement)
				return VisitDropSequence((DropSequenceStatement) statement);
			if (statement is DropProcedureStatement)
				return VisitDropProcedure((DropProcedureStatement) statement);
			if (statement is DropFunctionStatement)
				return VisitDropFunction((DropFunctionStatement) statement);
			if (statement is DropRoleStatement)
				return VisitDropRole((DropRoleStatement) statement);
			if (statement is DropUserStatement)
				return VisitDropUser((DropUserStatement) statement);
			if (statement is DropTriggerStatement)
				return VisitDropTrigger((DropTriggerStatement) statement);
			if (statement is DropTypeStatement)
				return VisitDropType((DropTypeStatement) statement);

			// ALTER
			if (statement is AlterTableStatement)
				return VisitAlterTable((AlterTableStatement) statement);
			if (statement is AlterUserStatement)
				return VisitAlterUser((AlterUserStatement) statement);

			if (statement is GrantPrivilegesStatement)
				return VisitGrantPrivilege((GrantPrivilegesStatement) statement);
			if (statement is GrantRoleStatement)
				return VisitGrantRole((GrantRoleStatement) statement);
			if (statement is RevokePrivilegesStatement)
				return VisitRevokePrivilege((RevokePrivilegesStatement) statement);
			// TODO: Revoke role statement

			// DECLARE
			if (statement is DeclareCursorStatement)
				return VisitDeclareCursor((DeclareCursorStatement) statement);
			if (statement is DeclareVariableStatement)
				return VisitDeclareVariable((DeclareVariableStatement) statement);
			if (statement is DeclareExceptionStatement)
				return VisitDeclareException((DeclareExceptionStatement) statement);

			if (statement is CallStatement)
				return VisitCall((CallStatement) statement);

			if (statement is InsertStatement)
				return VisitInsert((InsertStatement) statement);
			if (statement is InsertSelectStatement)
				return VisitInsertSelect((InsertSelectStatement) statement);
			if (statement is SelectStatement)
				return VisitSelect((SelectStatement) statement);
			if (statement is SelectIntoStatement)
				return VisitSelectInto((SelectIntoStatement) statement);
			if (statement is DeleteStatement)
				return VisitDelete((DeleteStatement) statement);
			if (statement is UpdateStatement)
				return VisitUpdate((UpdateStatement) statement);
			if (statement is UpdateFromCursorStatement)
				return VisitUpdateFromCursor((UpdateFromCursorStatement) statement);
			if (statement is ShowStatement)
				return VisitShow((ShowStatement) statement);

			// Cursors
			if (statement is OpenStatement)
				return VisitOpen((OpenStatement) statement);
			if (statement is FetchStatement)
				return VisitFetch((FetchStatement) statement);
			if (statement is CloseStatement)
				return VisitClose((CloseStatement) statement);

			// Session Commands
			if (statement is CommitStatement)
				return VisitCommit((CommitStatement) statement);
			if (statement is RollbackStatement)
				return VisitRollback((RollbackStatement) statement);
			if (statement is SetStatement)
				return VisitSet((SetStatement) statement);

			if (statement is AssignVariableStatement)
				return VisitAssignVariable((AssignVariableStatement) statement);

			// Blocks
			if (statement is CodeBlockStatement)
				return VisitCodeBlock((CodeBlockStatement) statement);

			// In-Block
			if (statement is LoopControlStatement)
				return VisitLoopControl((LoopControlStatement) statement);
			if (statement is ReturnStatement)
				return VisitReturn((ReturnStatement) statement);
			if (statement is GoToStatement)
				return VisitGoTo((GoToStatement) statement);
			if (statement is RaiseStatement)
				return VisitRaise((RaiseStatement) statement);

			if (statement is ConditionStatement)
				return VisitCondition((ConditionStatement) statement);

			if (statement is IVisitableStatement)
				return ((IVisitableStatement) statement).Accept(this);

			return statement;
		}

		protected virtual SqlStatement VisitCall(CallStatement statement) {
			return new CallStatement(statement.ProcedureName, statement.Arguments);
		}

		protected virtual SqlStatement VisitShow(ShowStatement statement) {
			return new ShowStatement(statement.Target, statement.TableName);
		}

		protected virtual SqlStatement VisitCondition(ConditionStatement statement) {
			var trueStatements = new SqlStatement[statement.TrueStatements.Length];
			for (int i = 0; i < trueStatements.Length; i++) {
				trueStatements[i] = VisitStatement(statement.TrueStatements[i]);
			}

			var falseStatements = statement.FalseStatements;
			if (falseStatements != null) {
				for (int i = 0; i < falseStatements.Length; i++) {
					falseStatements[i] = VisitStatement(falseStatements[i]);
				}
			}

			return new ConditionStatement(statement.ConditionExpression, trueStatements);
		}

		protected virtual SqlStatement VisitRaise(RaiseStatement statement) {
			return new RaiseStatement(statement.ExceptionName);
		}

		protected virtual SqlStatement VisitGoTo(GoToStatement statement) {
			return new GoToStatement(statement.Label);
		}

		protected virtual SqlStatement VisitReturn(ReturnStatement statement) {
			return new ReturnStatement(statement.ReturnExpression);
		}

		protected virtual SqlStatement VisitLoopControl(LoopControlStatement statement) {
			if (statement is ExitStatement)
				return VisitExit((ExitStatement) statement);
			if (statement is ContinueStatement)
				return VisitContinue((ContinueStatement) statement);
			
			return new LoopControlStatement(statement.ControlType, statement.Label, statement.WhenExpression);
		}

		protected virtual SqlStatement VisitExit(ExitStatement statement) {
			return new ExitStatement(statement.Label, statement.WhenExpression);
		}

		protected virtual SqlStatement VisitContinue(ContinueStatement statement) {
			return new ContinueStatement(statement.Label, statement.WhenExpression);
		}

		protected virtual SqlStatement VisitAssignVariable(AssignVariableStatement statement) {
			return new AssignVariableStatement(statement.VariableReference, statement.ValueExpression);
		}

		protected virtual SqlStatement VisitSet(SetStatement statement) {
			return new SetStatement(statement.SettingName, statement.ValueExpression);
		}

		protected virtual SqlStatement VisitRollback(RollbackStatement statement) {
			return new RollbackStatement();
		}

		protected virtual SqlStatement VisitCommit(CommitStatement statement) {
			return new CommitStatement();
		}

		protected virtual SqlStatement VisitClose(CloseStatement statement) {
			return new CloseStatement(statement.CursorName);
		}

		protected virtual SqlStatement VisitFetch(FetchStatement statement) {
			return new FetchStatement(statement.CursorName, statement.Direction, statement.OffsetExpression);
		}

		protected virtual SqlStatement VisitOpen(OpenStatement statement) {
			return new OpenStatement(statement.CursorName, statement.Arguments);
		}

		protected virtual SqlStatement VisitUpdateFromCursor(UpdateFromCursorStatement statement) {
			return new UpdateFromCursorStatement(statement.TableName, statement.CursorName);
		}

		protected virtual SqlStatement VisitUpdate(UpdateStatement statement) {
			return new UpdateStatement(statement.TableName, statement.WherExpression, statement.Assignments) {
				Limit = statement.Limit
			};
		}

		protected virtual SqlStatement VisitDelete(DeleteStatement statement) {
			return new DeleteStatement(statement.TableName, statement.WhereExpression, statement.Limit);
		}

		protected virtual SqlStatement VisitSelectInto(SelectIntoStatement statement) {
			return new SelectIntoStatement(statement.QueryExpression, statement.Reference);
		}

		protected virtual SqlStatement VisitSelect(SelectStatement statement) {
			return new SelectStatement(statement.QueryExpression, statement.Limit, statement.OrderBy);
		}

		protected virtual SqlStatement VisitInsertSelect(InsertSelectStatement statement) {
			return new InsertSelectStatement(statement.TableName, statement.ColumnNames, statement.QueryExpression);
		}

		protected virtual SqlStatement VisitInsert(InsertStatement statement) {
			return new InsertStatement(statement.TableName, statement.ColumnNames, statement.Values);
		}

		protected virtual SqlStatement VisitDeclareException(DeclareExceptionStatement statement) {
			return new DeclareExceptionStatement(statement.ExceptionName);
		}

		protected virtual SqlStatement VisitDeclareVariable(DeclareVariableStatement statement) {
			return new DeclareVariableStatement(statement.VariableName, statement.VariableType) {
				IsNotNull = statement.IsNotNull,
				IsConstant = statement.IsConstant,
				DefaultExpression = statement.DefaultExpression
			};
		}

		protected virtual SqlStatement VisitDeclareCursor(DeclareCursorStatement statement) {
			return new DeclareCursorStatement(statement.CursorName, statement.Parameters, statement.Flags, statement.QueryExpression);
		}

		protected virtual SqlStatement VisitRevokePrivilege(RevokePrivilegesStatement statement) {
			return new RevokePrivilegesStatement(statement.Grantee, statement.Privileges, statement.GrantOption,
				statement.ObjectName, statement.Columns);
		}

		protected virtual SqlStatement VisitGrantRole(GrantRoleStatement statement) {
			return new GrantRoleStatement(statement.Grantee, statement.Role, statement.WithAdmin);
		}

		protected virtual SqlStatement VisitGrantPrivilege(GrantPrivilegesStatement statement) {
			return new GrantPrivilegesStatement(statement.Grantee, statement.Privilege, statement.WithGrant, statement.ObjectName,
				statement.Columns);
		}

		protected virtual SqlStatement VisitAlterUser(AlterUserStatement statement) {
			return new AlterUserStatement(statement.UserName, statement.AlterAction);
		}

		protected virtual SqlStatement VisitAlterTable(AlterTableStatement statement) {
			return new AlterTableStatement(statement.TableName, statement.Action);
		}

		protected virtual SqlStatement VisitCodeBlock(CodeBlockStatement statement) {
			if (statement is LoopStatement)
				return VisitLoop((LoopStatement) statement);
			if (statement is PlSqlBlockStatement)
				return VisitPlSqlBlock((PlSqlBlockStatement) statement);

			return statement;
		}

		protected virtual SqlStatement VisitPlSqlBlock(PlSqlBlockStatement statement) {
			var block = new PlSqlBlockStatement();
			foreach (var declaration in statement.Declarations) {
				block.Declarations.Add(VisitStatement(declaration));
			}

			foreach (var child in statement.Statements) {
				block.Statements.Add(VisitStatement(child));
			}

			foreach (var handler in statement.ExceptionHandlers) {
				block.ExceptionHandlers.Add(handler);
			}

			return block;
		}

		protected virtual SqlStatement VisitLoop(LoopStatement statement) {
			if (statement is ForLoopStatement)
				return VisitForLoop((ForLoopStatement) statement);
			if (statement is WhileLoopStatement)
				return VisitWhileLoop((WhileLoopStatement) statement);
			if (statement is CursorForLoopStatement)
				return VisitCursorForLoop((CursorForLoopStatement) statement);

			var loop = new LoopStatement();
			foreach (var child in statement.Statements) {
				loop.Statements.Add(VisitStatement(child));
			}
			
			return loop;
		}

		protected virtual SqlStatement VisitCursorForLoop(CursorForLoopStatement statement) {
			var loop = new CursorForLoopStatement(statement.IndexName, statement.CursorName);
			foreach (var child in statement.Statements) {
				loop.Statements.Add(VisitStatement(child));
			}
			
			return loop;
		}

		protected virtual SqlStatement VisitWhileLoop(WhileLoopStatement statement) {
			var loop = new WhileLoopStatement(statement.ConditionExpression);

			foreach (var child in statement.Statements) {
				loop.Statements.Add(VisitStatement(child));
			}
			
			return loop;
		}

		private SqlStatement VisitForLoop(ForLoopStatement statement) {
			var loop = new ForLoopStatement(statement.IndexName, statement.LowerBound, statement.UpperBound);

			foreach (var child in statement.Statements) {
				loop.Statements.Add(VisitStatement(child));
			}
			
			return loop;
		}

		protected virtual SqlStatement VisitDropType(DropTypeStatement statement) {
			return new DropTypeStatement(statement.TypeName);
		}

		protected virtual SqlStatement VisitDropTrigger(DropTriggerStatement statement) {
			return new DropTriggerStatement(statement.TriggerName);
		}

		protected virtual SqlStatement VisitDropUser(DropUserStatement statement) {
			return new DropUserStatement(statement.UserName);
		}

		protected virtual SqlStatement VisitDropRole(DropRoleStatement statement) {
			return new DropRoleStatement(statement.RoleName);
		}

		protected virtual SqlStatement VisitDropFunction(DropFunctionStatement statement) {
			return new DropFunctionStatement(statement.FunctionName) {
				IfExists = statement.IfExists
			};
		}

		protected virtual SqlStatement VisitDropProcedure(DropProcedureStatement statement) {
			return new DropProcedureStatement(statement.ProcedureName) {
				IfExists = statement.IfExists
			};
		}

		protected virtual SqlStatement VisitDropSequence(DropSequenceStatement statement) {
			return new DropSequenceStatement(statement.SequenceName);
		}

		protected virtual SqlStatement VisitDropCallbackTrigger(DropCallbackTriggersStatement statement) {
			return new DropCallbackTriggersStatement(statement.TriggerName);
		}

		protected virtual SqlStatement VisitDropTable(DropTableStatement statement) {
			return new DropTableStatement(statement.TableName, statement.IfExists);
		}

		protected virtual SqlStatement VisitDropView(DropViewStatement statement) {
			return new DropViewStatement(statement.ViewName, statement.IfExists);
		}

		protected virtual SqlStatement VisitCreateExternFunction(CreateExternalFunctionStatement statement) {
			return new CreateExternalFunctionStatement(statement.FunctionName, statement.ReturnType, statement.Parameters,
				statement.ExternalReference) {
					ReplaceIfExists = statement.ReplaceIfExists
				};
		}

		protected virtual SqlStatement VisitCreateFunction(CreateFunctionStatement statement) {
			var body = statement.Body;
			if (body != null)
				// TODO: Maybe the body should be generic to support this model
				body = (PlSqlBlockStatement) VisitStatement(body);

			return new CreateFunctionStatement(statement.FunctionName, statement.ReturnType, statement.Parameters, body) {
				ReplaceIfExists = statement.ReplaceIfExists
			};
		}

		protected virtual SqlStatement VisitCreateExternProcedure(CreateExternalProcedureStatement statement) {
			return new CreateExternalProcedureStatement(statement.ProcedureName, statement.Parameters,
				statement.ExternalReference) {
					ReplaceIfExists = statement.ReplaceIfExists
				};
		}

		protected virtual SqlStatement VisitCreateProcedure(CreateProcedureStatement statement) {
			var body = statement.Body;
			if (body != null)
				// TODO: Maybe the body should be generic to support this model
				body = (PlSqlBlockStatement)VisitStatement(body);

			return new CreateProcedureStatement(statement.ProcedureName, statement.Parameters, body) {
				ReplaceIfExists = statement.ReplaceIfExists
			};
		}

		protected virtual SqlStatement VisitCreateRole(CreateRoleStatement statement) {
			return new CreateRoleStatement(statement.RoleName);
		}

		protected virtual SqlStatement VisitCreateUser(CreateUserStatement statement) {
			return new CreateUserStatement(statement.UserName, statement.Identifier);
		}

		protected virtual SqlStatement VisitCreateProcedureTrigger(CreateProcedureTriggerStatement statement) {
			return new CreateProcedureTriggerStatement(statement.TriggerName, statement.TableName, statement.ProcedureName,
				statement.ProcedureArguments, statement.EventTime, statement.EventType);
		}

		protected virtual SqlStatement VisitCreateCallbackTrigger(CreateCallbackTriggerStatement statement) {
			return new CreateCallbackTriggerStatement(statement.TriggerName, statement.TableName, statement.EventTime, statement.EventType);
		}

		protected virtual SqlStatement VisitCreateTrigger(CreateTriggerStatement statement) {
			var body = statement.Body;
			if (body != null)
				// TODO: Maybe the body should be generic to support this model
				body = (PlSqlBlockStatement)VisitStatement(body);

			return new CreateTriggerStatement(statement.TriggerName, statement.TableName, body, statement.EventTime, statement.EventType);
		}

		protected virtual SqlStatement VisitCreateSequence(CreateSequenceStatement statement) {
			return new CreateSequenceStatement(statement.SequenceName) {
				StartWith = statement.StartWith,
				MinValue = statement.MinValue,
				MaxValue = statement.MaxValue,
				IncrementBy = statement.IncrementBy,
				Cache = statement.Cache,
				Cycle = statement.Cycle
			};
		}

		protected virtual SqlStatement VisitCreateSchema(CreateSchemaStatement statement) {
			return new CreateSchemaStatement(statement.SchemaName);
		}

		protected virtual SqlStatement VisitCreateView(CreateViewStatement statement) {
			return new CreateViewStatement(statement.ViewName, statement.ColumnNames, statement.QueryExpression) {
				ReplaceIfExists = statement.ReplaceIfExists
			};
		}

		protected virtual SqlStatement VisitCreateTable(CreateTableStatement statement) {
			return new CreateTableStatement(statement.TableName, statement.Columns) {
				Temporary = statement.Temporary,
				IfNotExists = statement.IfNotExists
			};
		}
	}
}
