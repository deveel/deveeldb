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

using Antlr4.Runtime.Misc;

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Types;
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

		public override SqlStatement VisitNullStatement(PlSqlParser.NullStatementContext context) {
			return new NullStatement();
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
			var triggerName = Name.Object(context.objectName());
			var orReplace = context.OR() != null && context.REPLACE() != null;

			var simpleDml = context.simpleDmlTrigger();

			ObjectName onObject = null;
			TriggerEventType eventType = new TriggerEventType();
			TriggerEventTime eventTime = new TriggerEventTime();

			if (simpleDml != null) {
				bool before = simpleDml.BEFORE() != null;
				bool after = simpleDml.AFTER() != null;

				if (simpleDml.INSTEAD() != null && simpleDml.OF() != null)
					throw new NotSupportedException("The INSTEAD OF clause not yet supported.");

				var events = simpleDml.dmlEventClause().dmlEventElement().Select(x => {
					if (x.DELETE() != null)
						return TriggerEventType.Delete;
					if (x.UPDATE() != null)
						return TriggerEventType.Update;
					if (x.INSERT() != null)
						return TriggerEventType.Insert;

					throw new InvalidOperationException();
				});

				foreach (var type in events) {
					eventType |= type;
				}

				if (before) {
					eventTime = TriggerEventTime.Before;
				} else if (after) {
					eventTime = TriggerEventTime.After;
				}

				onObject = Name.Object(simpleDml.dmlEventClause().objectName());
			}

			var triggerBody = context.triggerBody();
			if (triggerBody.triggerBlock() != null) {
				var declarations = triggerBody.triggerBlock().declaration().Select(Visit);
				var body = (PlSqlBody) Visit(triggerBody.triggerBlock().body());

				var plsqlBody = body.AsPlSqlStatement();
				foreach (var declaration in declarations) {
					plsqlBody.Declarations.Add(declaration);
				}

				return new CreateTriggerStatement(triggerName, onObject, plsqlBody, eventTime, eventType);
			}

			var procName = Name.Object(triggerBody.objectName());
			var args = new InvokeArgument[0];
			if (triggerBody.function_argument() != null)
				args = triggerBody.function_argument()
					.argument()
					.Select(FunctionArgument.Form)
					.Select(x => new InvokeArgument(x.Id, x.Expression))
					.ToArray();

			return new CreateProcedureTriggerStatement(triggerName, onObject, procName, args, eventTime, eventType);
		}

		public override SqlStatement VisitCallStatement(PlSqlParser.CallStatementContext context) {
			var routineName = Name.Object(context.objectName());
			var args = new InvokeArgument[0];

			if (context.function_argument() != null)
				args = context.function_argument()
						.argument()
						.Select(FunctionArgument.Form)
						.Select(x => new InvokeArgument(x.Id, x.Expression))
						.ToArray();

			if (args.Length > 0) {
				bool named = args.Any(x => x.IsNamed);
				if (named && args.Any(x => !x.IsNamed))
					throw new ParseCanceledException("Anonymous argument mixed with named arguments");
			}

			return new CallStatement(routineName, args);
		}

		public override SqlStatement VisitCreateTypeStatement(PlSqlParser.CreateTypeStatementContext context) {
			var orReplace = context.OR() != null && context.REPLACE() != null;
			var typeName = Name.Object(context.objectName());
			ObjectName parentType = null;

			if (context.underClause() != null)
				parentType = Name.Object(context.underClause().objectName());

			bool isSealed = true, isAbstract = false;
			if (context.INSTANTIABLE() != null) {
				if (context.NOT() != null) {
					isAbstract = true;
				} else {
					isAbstract = false;
				}
			} else if (context.FINAL() != null) {
				if (context.NOT() != null) {
					isSealed = false;
				} else {
					isSealed = true;
				}
			}

			var members = new List<UserTypeMember>();
			foreach (var attributeContext in context.typeAttribute()) {
				var memberName = Name.Simple(attributeContext.id());
				var memberType = SqlTypeParser.Parse(attributeContext.datatype());

				members.Add(new UserTypeMember(memberName, memberType));
			}

			return new CreateTypeStatement(typeName, members.ToArray(), orReplace) {
				ParentTypeName = parentType,
				IsSealed = isSealed,
				IsAbstract = isAbstract
			};
		}

		public override SqlStatement VisitDropTypeStatement(PlSqlParser.DropTypeStatementContext context) {
			bool ifExists = (context.IF() != null && context.EXISTS() != null);
			var typeName = Name.Object(context.objectName());

			return new DropTypeStatement(typeName, ifExists);
		}

		public override SqlStatement VisitCreateSequenceStatement(PlSqlParser.CreateSequenceStatementContext context) {
			return SequenceStatements.Create(context);
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
			
			var statements = context.seqOfStatements().statement().Select(Visit);
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
			var statements = context.seqOfStatements().statement().Select(Visit);
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
				block = ((PlSqlBody) body).AsPlSqlStatement();
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

		public override SqlStatement VisitLoopStatement(PlSqlParser.LoopStatementContext context) {
			LoopStatement loop;

			if (context.WHILE() != null) {
				var condition = Expression.Build(context.condition());
				loop = new WhileLoopStatement(condition);
			} else if (context.FOR() != null) {
				var param = context.cursorLoopParam();
				if (param.lowerBound() != null &&
				    param.upperBound() != null) {
					var lower = Expression.Build(param.lowerBound());
					var upper = Expression.Build(param.upperBound());
					var indexName = Name.Simple(param.id());

					var reverse = param.REVERSE() != null;
					loop = new ForLoopStatement(indexName, lower, upper) {
						Reverse = reverse
					};
				} else {
					throw new NotImplementedException();
				}
			} else {
				loop = new LoopStatement();
			}

			if (context.labelDeclaration() != null) {
				var labelName = Name.Simple(context.labelDeclaration().id());
				loop.Label = labelName;
			}

			var seqOfStatements = context.seqOfStatements();
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

					direction = FetchDirection.Absolute;
					offset = value.Value;
				} else if (fetchDirection.RELATIVE() != null) {
					var value = Number.PositiveInteger(fetchDirection.numeric());
					if (value == null)
						throw new ParseCanceledException("FETCH RELATIVE requires a numeric offset.");

					direction = FetchDirection.Relative;
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
				privs = PrivilegeSets.TableAll;
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
				privs = PrivilegeSets.TableAll;
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

		public override SqlStatement VisitSimpleCaseStatement(PlSqlParser.SimpleCaseStatementContext context) {
			var exp = Expression.Build(context.atom());

			var switches = new List<CaseSwitch>();

			foreach (var partContext in context.simpleCaseWhenPart()) {
				var otherExp = Expression.Build(partContext.expressionWrapper());
				switches.Add(new CaseSwitch {
					Condition =SqlExpression.Equal(exp, otherExp),
					ReturnStatements = partContext.seqOfStatements().statement().Select(Visit).ToArray()
				});
			}

			if (context.caseElsePart() != null) {
				var returnstatements = context.caseElsePart().seqOfStatements().statement().Select(Visit).ToArray();
				switches.Add(new CaseSwitch {
					Condition = SqlExpression.Constant(true),
					ReturnStatements = returnstatements
				});
			}

			ConditionStatement conditional = null;

			for (int i = switches.Count - 1; i >= 0; i--) {
				var current = switches[i];

				var condition = new ConditionStatement(current.Condition, current.ReturnStatements);

				if (conditional != null) {
					conditional = new ConditionStatement(current.Condition, current.ReturnStatements, new SqlStatement[] {conditional});
				} else {
					conditional = condition;
				}
			}

			return conditional;
		}

		public override SqlStatement VisitSearchedCaseStatement(PlSqlParser.SearchedCaseStatementContext context) {
			var switches = new List<CaseSwitch>();

			foreach (var partContext in context.searchedCaseWhenPart()) {
				switches.Add(new CaseSwitch {
					Condition = Expression.Build(partContext.conditionWrapper()),
					ReturnStatements = partContext.seqOfStatements().statement().Select(Visit).ToArray()
				});
			}

			if (context.caseElsePart() != null) {
				var returnstatements = context.caseElsePart().seqOfStatements().statement().Select(Visit).ToArray();
				switches.Add(new CaseSwitch {
					Condition = SqlExpression.Constant(true),
					ReturnStatements = returnstatements
				});
			}

			ConditionStatement conditional = null;

			for (int i = switches.Count - 1; i >= 0; i--) {
				var current = switches[i];

				var condition = new ConditionStatement(current.Condition, current.ReturnStatements);

				if (conditional != null) {
					conditional = new ConditionStatement(current.Condition, current.ReturnStatements, new SqlStatement[] { conditional });
				} else {
					conditional = condition;
				}
			}

			return conditional;
		}

		#region CaseSwitch

		class CaseSwitch {
			public SqlExpression Condition { get; set; }

			public SqlStatement[] ReturnStatements { get; set; }
		}

		#endregion


		public override SqlStatement VisitReturnStatement(PlSqlParser.ReturnStatementContext context) {
			var returnContext = context.condition();
			SqlExpression returnExpression = null;
			if (returnContext != null)
				returnExpression = Expression.Build(returnContext);

			return new ReturnStatement(returnExpression);
		}

		public override SqlStatement VisitIfStatement(PlSqlParser.IfStatementContext context) {
			var conditions = new List<ConditionPart> {
				new ConditionPart {
					Condition = Expression.Build(context.condition()),
					Statements = context.seqOfStatements().statement().Select(Visit).ToArray()
				}
			};

			foreach (var partContext in context.elsifPart()) {
				conditions.Add(new ConditionPart {
					Condition = Expression.Build(partContext.condition()),
					Statements = partContext.seqOfStatements().statement().Select(Visit).ToArray()
				});
			}

			if (context.elsePart() != null) {
				var statements = context.elsePart().seqOfStatements().statement().Select(Visit).ToArray();
				conditions.Add(new ConditionPart {
					Condition = SqlExpression.Constant(true),
					Statements = statements
				});
			}

			ConditionStatement conditional = null;

			for (int i = conditions.Count - 1; i >= 0; i--) {
				var current = conditions[i];

				var condition = new ConditionStatement(current.Condition, current.Statements);

				if (conditional != null) {
					conditional = new ConditionStatement(current.Condition, current.Statements, new SqlStatement[] { conditional });
				} else {
					conditional = condition;
				}
			}

			return conditional;
		}

		#region Condition

		class ConditionPart {
			public SqlExpression Condition { get; set; }

			public SqlStatement[] Statements { get; set; }
		}

		#endregion

		public override SqlStatement VisitCreateFunctionStatement(PlSqlParser.CreateFunctionStatementContext context) {
			var functionName = Name.Object(context.objectName());
			bool orReplace = context.OR() != null && context.REPLACE() != null;

			var body = context.body();
			var call = context.callSpec();

			SqlType returnType;

			var returnTypeSpec = context.functionReturnType();
			if (returnTypeSpec.TABLE() != null) {
				returnType = new TabularType();
			} else if (returnTypeSpec.DETERMINISTIC() != null) {
				returnType = Function.DynamicType;
			} else {
				var typeInfo = new DataTypeVisitor().Visit(returnTypeSpec.primitive_type());
				if (!typeInfo.IsPrimitive)
					throw new NotSupportedException(String.Format("The return type of function '{0}' ('{1}') is not primitive.",
						functionName, typeInfo));

				returnType = PrimitiveTypes.Resolve(typeInfo.TypeName, typeInfo.Metadata);
			}

			RoutineParameter[] parameters = null;
			if (context.parameter() != null &&
				context.parameter().Length > 0) {
				parameters = context.parameter().Select(Parameter.Routine).ToArray();
			}

			if (body != null) {
				var functionBody = Visit(body);

				if (!(functionBody is PlSqlBody))
					throw new ParseCanceledException("Invalid function body.");

				var plsqlBody = ((PlSqlBody)functionBody).AsPlSqlStatement();

				var declarationArray = context.declaration();
				if (declarationArray != null && declarationArray.Length > 0) {
					foreach (var declContext in declarationArray) {
						var declaration = Visit(declContext);
						plsqlBody.Declarations.Add(declaration);
					}
				}

				return new CreateFunctionStatement(functionName, returnType, parameters, plsqlBody) {
					ReplaceIfExists = orReplace
				};
			}

			var typeString = InputString.AsNotQuoted(call.dotnetSpec().typeString.Text);
			var assemblyToken = InputString.AsNotQuoted(call.dotnetSpec().assemblyString);
			if (assemblyToken != null) {
				typeString = String.Format("{0}, {1}", typeString, assemblyToken);
			}

			return new CreateExternalFunctionStatement(functionName, returnType, parameters, typeString);
		}

		public override SqlStatement VisitCreateProcedureStatement(PlSqlParser.CreateProcedureStatementContext context) {
			var procedureName = Name.Object(context.objectName());
			bool orReplace = context.OR() != null && context.REPLACE() != null;

			var body = context.body();
			var call = context.callSpec();

			RoutineParameter[] parameters = null;
			if (context.parameter() != null &&
			    context.parameter().Length > 0) {
				parameters = context.parameter().Select(Parameter.Routine).ToArray();
			}

			if (body != null) {
				var plsqlBody = Visit(body);

				if (!(plsqlBody is PlSqlBody))
					throw new ParseCanceledException("Invalid procedure body.");

				var block = ((PlSqlBody) plsqlBody).AsPlSqlStatement();

				var declarationArray = context.declaration();
				if (declarationArray != null && declarationArray.Length > 0) {
					foreach (var declContext in declarationArray) {
						var declaration = Visit(declContext);
						block.Declarations.Add(declaration);
					}
				}

				return new CreateProcedureStatement(procedureName, parameters, block) {
					ReplaceIfExists = orReplace
				};
			}

			var typeString =  InputString.AsNotQuoted(call.dotnetSpec().typeString.Text);
			var assemblyToken = InputString.AsNotQuoted(call.dotnetSpec().assemblyString);
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
			var procName = Name.Object(context.objectName());
			bool ifExists = context.IF() != null && context.EXISTS() != null;
			return new DropProcedureStatement(procName, ifExists);
		}

		public override SqlStatement VisitDropFunctionStatement(PlSqlParser.DropFunctionStatementContext context) {
			var funcName = Name.Object(context.objectName());
			bool ifExists = context.IF() != null && context.EXISTS() != null;
			return new DropFunctionStatement(funcName, ifExists);
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
			var roleName = Name.Simple(context.regular_id());
			return new DropRoleStatement(roleName);
		}

		public override SqlStatement VisitCreateRoleStatement(PlSqlParser.CreateRoleStatementContext context) {
			var roleName = Name.Simple(context.regular_id());
			return new CreateRoleStatement(roleName);
		}

		public override SqlStatement VisitCursorDeclaration(PlSqlParser.CursorDeclarationContext context) {
			return Cursor.Declare(context);
		}

		public override SqlStatement VisitExceptionDeclaration(PlSqlParser.ExceptionDeclarationContext context) {
			var exceptionName = Name.Simple(context.exception_name());
			return new DeclareExceptionStatement(exceptionName);
		}

		public override SqlStatement VisitExceptionInit(PlSqlParser.ExceptionInitContext context) {
			var exceptionName = Name.Simple(context.exception_name());
			var errorNumber = Number.Integer(context.numeric());
			if (errorNumber == null)
				throw new ParseCanceledException("Could not find a valid error code");

			return new DeclareExceptionInitStatement(exceptionName, errorNumber.Value);
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
