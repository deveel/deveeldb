using System;
using System.Collections.Generic;
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Compile {
	class SqlStatementVisitor : PlSqlParserBaseVisitor<SqlStatement> {
		public override SqlStatement VisitCompilationUnit(PlSqlParser.CompilationUnitContext context) {
			var sequence = new SequenceOfStatements();

			foreach (var statementContext in context.unitStatement()) {
				var statement = Visit(statementContext);
				if (statement is SequenceOfStatements) {
					var childSequence = (SequenceOfStatements) statement;
					foreach (var childStatement in childSequence.Statements) {
						sequence.Statements.Add(childStatement);
					}
				} else {
					sequence.Statements.Add(statement);
				}
			}

			return sequence;
		}

		public override SqlStatement VisitCreateSchemaStatement(PlSqlParser.CreateSchemaStatementContext context) {
			var schemaName = Name.Simple(context.id());
			return new CreateSchemaStatement(schemaName);
		}

		public override SqlStatement VisitDropSchemaStatement(PlSqlParser.DropSchemaStatementContext context) {
			var schemaName = Name.Simple(context.id());
			return new DropSchemaStatement(schemaName);
		}

		public override SqlStatement VisitCreateTableStatement(PlSqlParser.CreateTableStatementContext context) {
			return CreateTableBuilder.Build(context);
		}

		public override SqlStatement VisitAlterTableStatement(PlSqlParser.AlterTableStatementContext context) {
			return AlterTableBuilder.Build(context);
		}

		public override SqlStatement VisitCreateViewStatement(PlSqlParser.CreateViewStatementContext context) {
			return ViewStatements.Create(context);
		}

		public override SqlStatement VisitCreateTriggerStatement(PlSqlParser.CreateTriggerStatementContext context) {
			return base.VisitCreateTriggerStatement(context);
		}

		public override SqlStatement VisitCreateSequenceStatement(PlSqlParser.CreateSequenceStatementContext context) {
			return SequenceStatements.Create(context);
		}

		public override SqlStatement VisitDmlStatement(PlSqlParser.DmlStatementContext context) {
			return Visit(context.children.FirstOrDefault());
		}

		public override SqlStatement VisitSelectStatement(PlSqlParser.SelectStatementContext context) {
			return SelectBuilder.Build(context);
		}

		public override SqlStatement VisitUpdateStatement(PlSqlParser.UpdateStatementContext context) {
			return UpdateBuilder.Build(context);
		}

		public override SqlStatement VisitDeleteStatement(PlSqlParser.DeleteStatementContext context) {
			return DeleteBuilder.Build(context);
		}

		public override SqlStatement VisitCommitStatement(PlSqlParser.CommitStatementContext context) {
			return new CommitStatement();
		}

		public override SqlStatement VisitRollbackStatement(PlSqlParser.RollbackStatementContext context) {
			return new RollbackStatement();
		}

		public override SqlStatement VisitAlterTriggerStatement(PlSqlParser.AlterTriggerStatementContext context) {
			throw new NotImplementedException();
		}

		public override SqlStatement VisitAlterStatement(PlSqlParser.AlterStatementContext context) {
			return Visit(context.children.First());
		}

		public override SqlStatement VisitContinueStatement(PlSqlParser.ContinueStatementContext context) {
			return LoopControlBuilder.Build(context);
		}

		public override SqlStatement VisitBody(PlSqlParser.BodyContext context) {
			var block = new PlSqlBody();

			if (context.labelDeclaration() != null) {
				var labelName = Name.Simple(context.labelDeclaration().id());
				block.Label = labelName;
			}
			
			var statements = context.seq_of_statements().statement().Select(Visit);
			foreach (var statement in statements) {
				block.Statements.Add(statement);
			}

			var exceptionClause = context.exceptionClause();
			if (exceptionClause != null) {
				var handlers = exceptionClause.exceptionHandler().Select(BuildExceptionHandler);
				foreach (var handler in handlers) {
					block.ExceptionHandlers.Add(handler);
				}
			}

			return block;
		}

		private ExceptionHandler BuildExceptionHandler(PlSqlParser.ExceptionHandlerContext context) {
			HandledExceptions handled;
			if (context.OTHERS() != null) {
				handled = HandledExceptions.Others;
			} else {
				var handledExceptions = context.id().Select(Name.Simple).ToArray();
				handled = new HandledExceptions(handledExceptions);
			}

			var handler = new ExceptionHandler(handled);

			// TODO: support labels
			var statements = context.seq_of_statements().statement().Select(Visit);
			foreach (var statement in statements) {
				handler.Statements.Add(statement);
			}

			return handler;
		}

		public override SqlStatement VisitBlock(PlSqlParser.BlockContext context) {
			var declarations = context.declaration().Select(Visit);

			var block = new PlSqlBlockStatement();

			foreach (var declaration in declarations) {
				block.Declarations.Add(declaration);
			}

			var body = Visit(context.body());
			if (body is PlSqlBody) {
				var plsqlBody = (PlSqlBody) body;

				block.Label = plsqlBody.Label;

				foreach (var statement in plsqlBody.Statements) {
					block.Statements.Add(statement);
				}

				foreach (var handler in plsqlBody.ExceptionHandlers) {
					block.ExceptionHandlers.Add(handler);
				}
			} else if (body is SequenceOfStatements) {
				var seq = (SequenceOfStatements) body;
				foreach (var statement in seq.Statements) {
					block.Statements.Add(statement);
				}
			}

			return block;
		}

		public override SqlStatement VisitAssignmentStatement(PlSqlParser.AssignmentStatementContext context) {
			return Assignment.Statement(context);
		}

		public override SqlStatement VisitCreateStatement(PlSqlParser.CreateStatementContext context) {
			return Visit(context.children.First());
		}

		public override SqlStatement VisitCloseStatement(PlSqlParser.CloseStatementContext context) {
			return Cursor.Close(context);
		}

		public override SqlStatement VisitGotoStatement(PlSqlParser.GotoStatementContext context) {
			var label = Name.Simple(context.labelName());
			return new GoToStatement(label);
		}

		public override SqlStatement VisitForallStatement(PlSqlParser.ForallStatementContext context) {
			return base.VisitForallStatement(context);
		}

		public override SqlStatement VisitLoopStatement(PlSqlParser.LoopStatementContext context) {
			LoopStatement loop;

			if (context.WHILE() != null) {
				var condition = Expression.Build(context.condition());
				loop = new WhileLoopStatement(condition);
			} else if (context.FOR() != null) {
				throw new NotImplementedException();
			} else {
				loop = new LoopStatement();
			}

			if (context.labelDeclaration() != null) {
				var labelName = Name.Simple(context.labelDeclaration().id());
				loop.Label = labelName;
			}

			var seqOfStatements = context.seq_of_statements();
			if (seqOfStatements != null) {
				var statements = seqOfStatements.statement().Select(Visit);
				foreach (var statement in statements) {
					loop.Statements.Add(statement);
				}
			}

			return loop;
		}

		public override SqlStatement VisitDropTableStatement(PlSqlParser.DropTableStatementContext context) {
			return DropTableBuilder.Build(context);
		}

		public override SqlStatement VisitExitStatement(PlSqlParser.ExitStatementContext context) {
			return LoopControlBuilder.Build(context);
		}

		public override SqlStatement VisitFetchStatement(PlSqlParser.FetchStatementContext context) {
			string cursorName = Name.Simple(context.cursor_name());
			FetchDirection direction = FetchDirection.Next;
			int offset = -1;

			var fetchDirection = context.fetchDirection();
			if (fetchDirection != null) {
				if (fetchDirection.ABSOLUTE() != null) {
					var value = Number.PositiveInteger(fetchDirection.numeric());
					if (value == null)
						throw new ParseCanceledException("FETCH ABSOLUTE requires a numeric offset.");

					offset = value.Value;
				} else if (fetchDirection.RELATIVE() != null) {
					var value = Number.PositiveInteger(fetchDirection.numeric());
					if (value == null)
						throw new ParseCanceledException("FETCH RELATIVE requires a numeric offset.");

					offset = value.Value;
				} else if (fetchDirection.NEXT() != null) {
					direction = FetchDirection.Next;
				} else if (fetchDirection.PRIOR() != null) {
					direction = FetchDirection.Prior;
				} else if (fetchDirection.FIRST() != null) {
					direction = FetchDirection.First;
				} else if (fetchDirection.LAST() != null) {
					direction = FetchDirection.Last;
				}
			}

			var offsetExp = offset == -1 ? null : SqlExpression.Constant(offset);

			if (context.INTO() != null) {
				SqlExpression refExpression;
				var tableName = Name.Object(context.objectName());
				var varNames = context.variable_name().Select(Name.Simple).ToArray();
				if (tableName != null) {
					refExpression = SqlExpression.Reference(tableName);
				} else {
					refExpression = SqlExpression.Tuple(varNames.Select(SqlExpression.VariableReference).Cast<SqlExpression>().ToArray());
				}

				return new FetchIntoStatement(cursorName, direction, offsetExp, refExpression);
			}

			return new FetchStatement(cursorName, direction, offsetExp);
		}

		public override SqlStatement VisitDropTriggerStatement(PlSqlParser.DropTriggerStatementContext context) {
			var names = context.objectName().Select(Name.Object).ToArray();
			var callback = context.CALLBACK() != null;

			if (callback) {
				if (names.Length == 1)
					return new DropCallbackTriggersStatement(names[0].Name);

				var seq = new SequenceOfStatements();
				foreach (var name in names) {
					seq.Statements.Add(new DropCallbackTriggersStatement(name.Name));
				}

				return seq;
			} else {
				if (names.Length == 1)
					return new DropTriggerStatement(names[0]);

				var seq = new SequenceOfStatements();
				foreach (var name in names) {
					seq.Statements.Add(new DropTriggerStatement(name));
				}

				return seq;
			}
		}

		public override SqlStatement VisitOpenStatement(PlSqlParser.OpenStatementContext context) {
			return Cursor.Open(context);
		}

		public override SqlStatement VisitRaiseStatement(PlSqlParser.RaiseStatementContext context) {
			var exceptionName = Name.Simple(context.id());
			return new RaiseStatement(exceptionName);
		}

		public override SqlStatement VisitGrantPrivilegeStatement(PlSqlParser.GrantPrivilegeStatementContext context) {
			var privs = Privileges.None;

			if (context.ALL() != null) {
				privs = Privileges.TableAll;
			} else {
				var privNames = context.privilegeName().Select(x => x.GetText());
				foreach (var privName in privNames) {
					Privileges priv;

					try {
						priv = (Privileges) Enum.Parse(typeof (Privileges), privName, true);
					} catch (Exception) {
						throw new ParseCanceledException("Invalid privilege specified.");
					}

					privs |= priv;
				}
			}

			var withGrant = context.WITH() != null && context.GRANT() != null;
			var grantee = Name.Simple(context.granteeName());
			var objectName = Name.Object(context.objectName());

			return new GrantPrivilegesStatement(grantee, privs, withGrant, objectName);
		}

		public override SqlStatement VisitGrantRoleStatement(PlSqlParser.GrantRoleStatementContext context) {
			var grantee = Name.Simple(context.granteeName());
			var roleNames = context.roleName().Select(Name.Simple).ToArray();
			if (roleNames.Length == 1)
				return new GrantRoleStatement(grantee, roleNames[0]);

			var seq = new SequenceOfStatements();
			foreach (var roleName in roleNames) {
				seq.Statements.Add(new GrantRoleStatement(grantee, roleName));
			}

			return seq;
		}

		public override SqlStatement VisitRevokePrivilegeStatement(PlSqlParser.RevokePrivilegeStatementContext context) {
			var granteeName = Name.Simple(context.granteeName());
			var objectName = Name.Object(context.objectName());
			var grantOption = context.GRANT() != null && context.OPTION() != null;

			Privileges privs = Privileges.None;
			if (context.ALL() != null) {
				privs = Privileges.TableAll;
			} else {
				var privNames = context.privilegeName().Select(x => x.GetText());

				foreach (var privName in privNames) {
					Privileges priv;

					try {
						priv = (Privileges)Enum.Parse(typeof(Privileges), privName, true);
					} catch (Exception) {
						throw new ParseCanceledException("Invalid privilege specified.");
					}

					privs |= priv;
				}
			}

			return new RevokePrivilegesStatement(granteeName, privs, grantOption, objectName, new string[0]);
		}

		public override SqlStatement VisitRevokeRoleStatement(PlSqlParser.RevokeRoleStatementContext context) {
			var grantee = Name.Simple(context.granteeName());
			var roleNames = context.roleName().Select(Name.Simple).ToArray();
			if (roleNames.Length == 1)
				return new RevokeRoleStatement(grantee, roleNames[0]);

			var seq = new SequenceOfStatements();
			foreach (var roleName in roleNames) {
				seq.Statements.Add(new RevokeRoleStatement(grantee, roleName));
			}

			return seq;
		}

		public override SqlStatement VisitTransactionControlStatement(PlSqlParser.TransactionControlStatementContext context) {
			return base.VisitTransactionControlStatement(context);
		}

		public override SqlStatement VisitSetIgnoreCase(PlSqlParser.SetIgnoreCaseContext context) {
			bool ignoreCase = true;
			if (context.ON() != null) {
				ignoreCase = true;
			} else if (context.OFF() != null) {
				ignoreCase = false;
			}

			return new SetStatement(TransactionSettingKeys.IgnoreIdentifiersCase, SqlExpression.Constant(ignoreCase));
		}

		public override SqlStatement VisitSetIsolationLevel(PlSqlParser.SetIsolationLevelContext context) {
			string level;
			if (context.SERIALIZABLE() != null) {
				level = "Serializable";
			} else if (context.READ() != null) {
				if (context.COMMITTED() != null) {
					level = "ReadCommitted";
				} else if (context.UNCOMMITTED() != null) {
					level = "ReadUncommitted";
				} else {
					throw new ParseCanceledException();
				}
			} else {
				throw new ParseCanceledException("Invalid isolation level.");
			}

			return new SetStatement(TransactionSettingKeys.IsolationLevel, SqlExpression.Constant(level));
		}

		public override SqlStatement VisitSetTransactionAccess(PlSqlParser.SetTransactionAccessContext context) {
			bool readOnly = true;
			if (context.READ() != null &&
			    context.WRITE() != null) {
				readOnly = false;
			} else if (context.READ() != null &&
			           context.ONLY() != null) {
				readOnly = true;
			}

			return new SetStatement(TransactionSettingKeys.ReadOnly, SqlExpression.Constant(readOnly));
		}

		public override SqlStatement VisitShowStatement(PlSqlParser.ShowStatementContext context) {
			ShowTarget target;
			ObjectName tableName = null;
			if (context.SCHEMA() != null) {
				target = ShowTarget.Schema;
			} else if (context.TABLES() != null) {
				target = ShowTarget.SchemaTables;
			} else if (context.TABLE() != null) {
				target = ShowTarget.Table;
				tableName = Name.Object(context.objectName());
			} else if (context.OPEN() != null &&
			           context.SESSIONS() != null) {
				target = ShowTarget.OpenSessions;
			} else if (context.SESSION() != null) {
				target = ShowTarget.Session;
			} else {
				throw new NotSupportedException();
			}

			return new ShowStatement(target, tableName);
		}

		public override SqlStatement VisitInsertStatement(PlSqlParser.InsertStatementContext context) {
			if (context.multiTableInsert() != null)
				throw new NotImplementedException();

			var singleTableInsert = context.singleTableInsert();
			var insertInto = singleTableInsert.insertIntoClause();

			if (insertInto != null) {
				var tableName = Name.Object(insertInto.objectName());
				var columnNames = new string[0];

				if (insertInto.columnName() != null) {
					columnNames = insertInto.columnName().Select(Name.Simple).ToArray();
				}

				if (singleTableInsert.valuesClause() != null) {
					var values = new List<SqlExpression[]>();
					foreach (var listContext in singleTableInsert.valuesClause().expression_list()) {
						var array = listContext.expression().Select(Expression.Build).ToArray();
						values.Add(array);
					}

					return new InsertStatement(tableName, columnNames, values);
				}

				if (singleTableInsert.subquery() != null) {
					var query = Subquery.Form(singleTableInsert.subquery());
					return new InsertSelectStatement(tableName, columnNames, query);
				}
			} else if (singleTableInsert.insertSetClause() != null) {
				var tableName = Name.Object(singleTableInsert.insertSetClause().objectName());

				var columns = new List<string>();
				var values = new List<SqlExpression>();

				foreach (var assignmentContext in singleTableInsert.insertSetClause().insertAssignment()) {
					var columnName = Name.Simple(assignmentContext.columnName());
					var value = Expression.Build(assignmentContext.expression());

					columns.Add(columnName);
					values.Add(value);
				}

				return new InsertStatement(tableName, columns.ToArray(), new[] {values.ToArray()});
			}

			return base.VisitInsertStatement(context);
		}

		public override SqlStatement VisitOpenForStatement(PlSqlParser.OpenForStatementContext context) {
			return base.VisitOpenForStatement(context);
		}

		public override SqlStatement VisitReturnStatement(PlSqlParser.ReturnStatementContext context) {
			var returnContext = context.condition();
			SqlExpression returnExpression = null;
			if (returnContext != null)
				returnExpression = Expression.Build(returnContext);

			return new ReturnStatement(returnExpression);
		}

		public override SqlStatement VisitIfStatement(PlSqlParser.IfStatementContext context) {
			var condition = Expression.Build(context.condition());
			var ifTrue = context.seq_of_statements().statement().Select(Visit).ToArray();

			SqlStatement[] ifFalse = null;

			var elsif = context.elsifPart();
			if (elsif != null && elsif.Length > 0) {
				// TODO: !!!
				throw new NotImplementedException();
			}

			if (context.elsePart() != null) {
				ifFalse = context.elsePart().seq_of_statements().statement().Select(Visit).ToArray();
			}

			return new ConditionStatement(condition, ifTrue, ifFalse);
		}

		public override SqlStatement VisitCreateFunctionBody(PlSqlParser.CreateFunctionBodyContext context) {
			return base.VisitCreateFunctionBody(context);
		}

		public override SqlStatement VisitCreateProcedureBody(PlSqlParser.CreateProcedureBodyContext context) {
			var procedureName = Name.Object(context.objectName());
			bool orReplace = context.OR() != null && context.REPLACE() != null;

			var body = context.body();
			var call = context.call_spec();

			RoutineParameter[] parameters = null;
			if (context.parameter() != null &&
			    context.parameter().Length > 0) {
				parameters = context.parameter().Select(Parameter.Routine).ToArray();
			}

			if (body != null) {
				var plsqlBody = Visit(body);

				if (!(plsqlBody is PlSqlBlockStatement))
					throw new ParseCanceledException("Invalid procedure body.");

				return new CreateProcedureStatement(procedureName, parameters, plsqlBody) {
					ReplaceIfExists = orReplace
				};
			}

			var typeString = call.dotnet_spec().typeString.Text;
			var assemblyToken = call.dotnet_spec().assemblyString;
			if (assemblyToken != null) {
				typeString = String.Format("{0}, {1}", typeString, assemblyToken);
			}

			return new CreateExternalProcedureStatement(procedureName, parameters, typeString);
		}

		public override SqlStatement VisitAlterUserStatement(PlSqlParser.AlterUserStatementContext context) {
			return UserStatements.Alter(context);
		}

		public override SqlStatement VisitCreateUserStatement(PlSqlParser.CreateUserStatementContext context) {
			return UserStatements.Create(context);
		}

		public override SqlStatement VisitDropProcedureStatement(PlSqlParser.DropProcedureStatementContext context) {
			return base.VisitDropProcedureStatement(context);
		}

		public override SqlStatement VisitDropFunctionStatement(PlSqlParser.DropFunctionStatementContext context) {
			return base.VisitDropFunctionStatement(context);
		}

		public override SqlStatement VisitDropSequenceStatement(PlSqlParser.DropSequenceStatementContext context) {
			return SequenceStatements.Drop(context);
		}

		public override SqlStatement VisitDropUserStatement(PlSqlParser.DropUserStatementContext context) {
			var userName = context.userName().GetText();
			return new DropUserStatement(userName);
		}

		public override SqlStatement VisitDropViewStatement(PlSqlParser.DropViewStatementContext context) {
			return ViewStatements.Drop(context);
		}

		public override SqlStatement VisitDropRoleStatement(PlSqlParser.DropRoleStatementContext context) {
			return RoleStatements.Drop(context);
		}

		public override SqlStatement VisitCreateRoleStatement(PlSqlParser.CreateRoleStatementContext context) {
			return RoleStatements.Create(context);
		}

		public override SqlStatement VisitGrantStatement(PlSqlParser.GrantStatementContext context) {
			return base.VisitGrantStatement(context);
		}

		public override SqlStatement VisitRevokeStatement(PlSqlParser.RevokeStatementContext context) {
			return base.VisitRevokeStatement(context);
		}

		public override SqlStatement VisitCursorDeclaration(PlSqlParser.CursorDeclarationContext context) {
			return Cursor.Declare(context);
		}

		public override SqlStatement VisitExceptionDeclaration(PlSqlParser.ExceptionDeclarationContext context) {
			var exceptionName = Name.Simple(context.id());
			return new DeclareExceptionStatement(exceptionName);
		}

		public override SqlStatement VisitVariableDeclaration(PlSqlParser.VariableDeclarationContext context) {
			var name = Name.Simple(context.variable_name());
			var type = SqlTypeParser.Parse(context.datatype());

			bool notNull = context.NOT() != null && context.NULL() != null;
			bool constant = context.CONSTANT() != null;

			if (constant)
				notNull = true;

			SqlExpression defaultValue = null;
			if (context.defaultValuePart() != null) {
				defaultValue = Expression.Build(context.defaultValuePart().expression());
			}

			return new DeclareVariableStatement(name, type) {
				DefaultExpression = defaultValue,
				IsNotNull = notNull,
				IsConstant = constant
			};
		}
	}
}
