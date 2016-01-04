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

using Irony.Parsing;

namespace Deveel.Data.Sql.Parser {
	partial class SqlGrammar : SqlGrammarBase {
		public override string Dialect {
			get { return "SQL-99"; }
		}

		protected override NonTerminal MakeRoot() {
			var root = new NonTerminal("root");
			Productions(root);

			return root;
		}

		private void Productions(NonTerminal root) {
			// PL/SQL
			root.Rule = SqlStatementList() + Eof |
			            SqlBlockList() + Eof;
		}

		private NonTerminal SqlBlockList() {
			var block = PlSqlBlock();
			var blockList = new NonTerminal("block_list");
			blockList.Rule = MakePlusRule(blockList, block);

			return blockList;
		}

		private NonTerminal StatementEnd() {
			var statementEnd = new NonTerminal("statement_end");
			statementEnd.Rule = Empty | ";";
			return statementEnd;
		}

		private NonTerminal OrReplace() {
			var orReplaceOpt = new NonTerminal("or_replace_opt");
			orReplaceOpt.Rule = Empty | OR + REPLACE;
			return orReplaceOpt;
		}

		private NonTerminal SqlStatementList() {
			var commandList = new NonTerminal("command_list", typeof(SequenceOfStatementsNode));

			commandList.Rule = MakePlusRule(commandList, SqlStatement());

			return commandList;
		}

		private NonTerminal SqlStatement() {
			var sqlStatement = new NonTerminal("sql_statement");

			var command = new NonTerminal("command");

			sqlStatement.Rule = command + StatementEnd();

			command.Rule =
				VariableDeclaration() |
				CursorDeclaration() |
				Create() |
				Alter() |
				Drop() |
				Declare() |
				Open() |
				Close() |
				Fetch() |
				Select() |
				Insert() |
				Update() |
				Delete() |
				Truncate() |
				Set() |
				Commit() |
				Rollback() |
				Grant() |
				Revoke();

			return sqlStatement;
		}

		private NonTerminal Declare() {
			var command = new NonTerminal("declare");

			command.Rule = DeclareCursor() |
			               DeclareVariable() |
						   DeclareException();

			return command;
		}

		private NonTerminal CursorDeclaration() {
			var declareCursor = new NonTerminal("cursor_declaration", typeof(DeclareCursorNode));
			var insensitiveOpt = new NonTerminal("insensitive_opt");
			var scrollOpt = new NonTerminal("scroll_opt");
			var cursorArgsOpt = new NonTerminal("cursor_args_opt");
			var cursorArgs = new NonTerminal("cursor_args");
			var cursorArg = new NonTerminal("cursor_arg", typeof(CursorParameterNode));
			declareCursor.Rule = Key("CURSOR") + Identifier + cursorArgsOpt + 
				insensitiveOpt + scrollOpt +
				Key("IS") + SqlQueryExpression();
			cursorArgsOpt.Rule = Empty | "(" + cursorArgs + ")";
			cursorArgs.Rule = MakePlusRule(cursorArgs, Comma, cursorArg);
			cursorArg.Rule = Identifier + DataType();
			insensitiveOpt.Rule = Empty | Key("INSENSITIVE");
			scrollOpt.Rule = Empty | Key("SCROLL");
			return declareCursor;
		}

		private NonTerminal DeclareCursor() {
			var declareCuror = new NonTerminal("declare_cursor");
			declareCuror.Rule = Key("DECLARE") + CursorDeclaration();
			return declareCuror;
		}

		private NonTerminal VariableDeclaration() {
			var declareVariable = new NonTerminal("variable_declaration", typeof(DeclareVariableNode));
			var constantOpt = new NonTerminal("constant_opt");
			var varNotNullOpt = new NonTerminal("var_not_null_opt");
			var varDefaultOpt = new NonTerminal("var_default_opt");
			var varDefaultAssign = new NonTerminal("var_default_assign");

			declareVariable.Rule = Identifier + constantOpt + DataType() + varNotNullOpt + varDefaultOpt;
			constantOpt.Rule = Empty | Key("CONSTANT");
			varNotNullOpt.Rule = Empty | Key("NOT") + Key("NULL");
			varDefaultOpt.Rule = Empty | varDefaultAssign + SqlExpression();
			varDefaultAssign.Rule = ":=" | Key("DEFAULT");

			return declareVariable;
		}

		private NonTerminal ExceptionDeclaration() {
			var declareException = new NonTerminal("exception_declaration", typeof(DeclareExceptionNode));
			declareException.Rule = Identifier + Key("EXCEPTION");
			return declareException;
		}

		private NonTerminal DeclareException() {
			var command = new NonTerminal("declare_exception");
			command.Rule = Key("DECLARE") + ExceptionDeclaration();
			return command;
		}

		private NonTerminal DeclareVariable() {
			var declareVariable = new NonTerminal("declare_variable");
			declareVariable.Rule = Key("DECLARE") + VariableDeclaration();
			return declareVariable;
		}

		private NonTerminal Open() {
			var openCommand = new NonTerminal("open_command", typeof(OpenCursorStatementNode));
			var argsOpt = new NonTerminal("args_opt");
			var argList = new NonTerminal("arg_list");

			openCommand.Rule = Key("OPEN") + Identifier + argsOpt;
			argsOpt.Rule = Empty | "(" + argList + ")";
			argList.Rule = MakePlusRule(argList, Comma, SqlExpression());
			return openCommand;
		}

		private NonTerminal Close() {
			var closeCommand = new NonTerminal("close_command", typeof(CloseCursorStatementNode));
			closeCommand.Rule = Key("CLOSE") + Identifier;
			return closeCommand;
		}

		private NonTerminal Fetch() {
			var fetchCommand=new NonTerminal("fetch_command", typeof(FetchStatementNode));
			var directionOpt = new NonTerminal("direction_opt");
			var fetchDirection = new NonTerminal("fetch_direction");
			var identOpt = new NonTerminal("ident_opt");
			identOpt.Flags = TermFlags.IsTransient;
			var fromOpt = new NonTerminal("from_opt");
			var intoOpt = new NonTerminal("into_opt");

			fetchCommand.Rule = Key("FETCH") + directionOpt + fromOpt + identOpt + intoOpt;
			directionOpt.Rule = Empty | fetchDirection;
			identOpt.Rule = Empty | Identifier;
			fetchDirection.Rule = Key("NEXT") |
			                      Key("PRIOR") |
			                      Key("FIRST") |
			                      Key("LAST") |
			                      Key("ABSOLUTE") + SqlExpression() |
			                      Key("RELATIVE") + SqlExpression();
			fromOpt.Rule = Empty | Key("FROM");
			intoOpt.Rule = Empty | Key("INTO") + SqlExpression();
			return fetchCommand;
		}

		private NonTerminal NestedSqlStatement() {
			var sqlStatement = new NonTerminal("sql_statement");

			sqlStatement.Rule = Select() |
					Insert() |
					Update() |
					Delete() |
					Open() |
					Close() |
					Fetch() |
					Commit() |
					Set() |
					Rollback();

			return sqlStatement;
		}

		private NonTerminal PlSqlBlock() {
			var plsqlBlock = new NonTerminal("plsql_block", typeof(PlSqlBlockNode));
			var labelStatement = new NonTerminal("plsql_label", typeof(LabelNode));
			var labelStatementOpt = new NonTerminal("plsql_label_opt");
			var declareStatementOpt = new NonTerminal("declare_statement_opt");
			var declareSpec = new NonTerminal("declare_spec");
			var declareCommand = new NonTerminal("declare_command");
			var declareSpecList = new NonTerminal("declare_spec_list");
			var plsqlCodeBlock = new NonTerminal("plsql_code_block", typeof(PlSqlCodeBlockNode));
			var sqlStatementList = new NonTerminal("sql_statement_list");
			var declarePragma = new NonTerminal("declare_pragma", typeof(DeclarePragmaNode));
			var exceptionBlockOpt = new NonTerminal("exception_block_opt");
			var exceptionBlock = new NonTerminal("exception_block");
			var exceptionHandler = new NonTerminal("exception_handler", typeof(ExceptionHandlerNode));
			var exceptionHandlerList = new NonTerminal("exception_handler_list");
			var exceptionNames = new NonTerminal("exception_names");
			var handledExceptions = new NonTerminal("handled_exceptions");
			var whenOpt = new NonTerminal("when_opt");

			whenOpt.Rule = Empty | Key("WHEN") + SqlExpression();

			#region PL/SQL Statement

			var plsqlStatement = new NonTerminal("plsql_statement");
			var plsqlCommand = new NonTerminal("plsql_command");

			var plsqlStatementList = new NonTerminal("plsql_statement_list");
			plsqlStatementList.Rule = MakePlusRule(plsqlStatementList, plsqlStatement);

			#region Conditional

			var conditionalStatement = new NonTerminal("conditional_statement");
			var conditionalElsifList = new NonTerminal("conditional_elsif_list");
			var conditionalElsif = new NonTerminal("conditional_elsif");
			var conditionalElseOpt = new NonTerminal("conditional_else_opt");

			conditionalStatement.Rule = IF + SqlExpression() + THEN + plsqlStatementList +
										conditionalElsifList +
										conditionalElseOpt +
										END + IF;
			conditionalElsifList.Rule = MakeStarRule(conditionalElsifList, conditionalElsif);
			conditionalElsif.Rule = ELSIF + SqlExpression() + THEN + plsqlStatementList;
			conditionalElseOpt.Rule = Empty | ELSE + plsqlStatementList;

			#endregion

			#region Loop

			var loopStatement = new NonTerminal("loop_statement");
			var loopLabelOpt = new NonTerminal("loop_label_opt");
			var loopHeadOpt = new NonTerminal("loop_head_opt");
			var loopHead = new NonTerminal("loop_head");
			var whileLoop = new NonTerminal("while_loop");
			var forLoop = new NonTerminal("for_loop");
			var numericLoopParam = new NonTerminal("numeric_loop_param");
			var reverseOpt = new NonTerminal("reverse_opt");
			var forLoopParam = new NonTerminal("for_loop_param");
			var cursorLoopParam = new NonTerminal("cursor_loop_param");
			var loopBodyList = new NonTerminal("loop_body_list");
			var loopBody = new NonTerminal("loop_body");
			var loopControlStatement = new NonTerminal("loop_control_statement");
			var labelOpt = new NonTerminal("label_opt");

			labelOpt.Rule = Empty | Identifier;

			#region Exit

			var exitStatement = new NonTerminal("exit_statement", typeof(ExitStatementNode));

			exitStatement.Rule = Key("EXIT") + labelOpt + whenOpt + StatementEnd();

			#endregion

			#region Break

			var breakStatement = new NonTerminal("break_statement", typeof(BreakStatementNode));

			breakStatement.Rule = Key("BREAK") + labelOpt + whenOpt + StatementEnd();

			#endregion

			#region Continue

			var continueStatement = new NonTerminal("continue_statement", typeof(ContinueStatementNode));

			continueStatement.Rule = Key("CONTINUE") + labelOpt + whenOpt + StatementEnd();

			#endregion

			loopStatement.Rule = loopLabelOpt + loopHeadOpt + Key("LOOP") + loopBodyList + Key("LOOP") + Key("END");
			loopLabelOpt.Rule = Empty | Identifier;
			loopBody.Rule = plsqlStatement | loopControlStatement;
			loopBodyList.Rule = MakePlusRule(loopBodyList, loopBody);
			loopControlStatement.Rule = exitStatement |
			                            breakStatement |
			                            continueStatement;
			loopHeadOpt.Rule = Empty | loopHead;
			loopHead.Rule = whileLoop | forLoop;
			whileLoop.Rule = Key("WHILE") + SqlExpression();
			forLoop.Rule = Key("FOR") + forLoopParam;
			forLoopParam.Rule = numericLoopParam | cursorLoopParam;
			numericLoopParam.Rule = Identifier + Key("IN") + reverseOpt + SqlExpression() + ".." + SqlExpression();
			reverseOpt.Rule = Empty | Key("REVERSE");
			cursorLoopParam.Rule = Identifier + Key("IN") + ObjectName();

			#endregion

			#region Raise

			var raiseStatement = new NonTerminal("raise_statement", typeof(RaiseStatementNode));
			var exceptionNameOpt = new NonTerminal("exception_name_opt");
			raiseStatement.Rule = Key("RAISE") + exceptionNameOpt;
			exceptionNameOpt.Rule = Empty | Identifier;

			#endregion

			#region Return

			var returnStatement = new NonTerminal("return_statement", typeof(ReturnStatementNode));
			returnStatement.Rule = Key("RETURN") + SqlExpression();

			#endregion

			#region GoTo

			var gotoStatement = new NonTerminal("goto_statement", typeof(GotoStatementNode));
			gotoStatement.Rule = Key("GOTO") + StringLiteral;

			#endregion

			plsqlStatement.Rule = plsqlCommand + StatementEnd();
			plsqlCommand.Rule = NestedSqlStatement() |
								gotoStatement |
								returnStatement |
								raiseStatement |
								loopStatement |
								conditionalStatement;

			#endregion

			plsqlBlock.Rule = labelStatementOpt + declareStatementOpt + plsqlCodeBlock;
			labelStatementOpt.Rule = Empty | labelStatement;
			labelStatement.Rule = "<<" + Identifier + ">>";
			declareStatementOpt.Rule = Empty | DECLARE + declareSpecList;
			declareSpecList.Rule = MakePlusRule(declareSpecList, declareSpec);
			declareCommand.Rule = VariableDeclaration() |
			                      ExceptionDeclaration() |
			                      declarePragma |
			                      CursorDeclaration();
			declareSpec.Rule = declareCommand + StatementEnd();
			declarePragma.Rule = Key("PRAGMA") + Key("EXCEPTION_INIT") + "(" + StringLiteral + "," + PositiveLiteral + ")";

			plsqlCodeBlock.Rule = Key("BEGIN") + plsqlStatementList + exceptionBlockOpt + Key("END");

			sqlStatementList.Rule = MakePlusRule(sqlStatementList, NestedSqlStatement());

			exceptionBlockOpt.Rule = Empty | exceptionBlock;
			exceptionBlock.Rule = Key("EXCEPTION") + exceptionHandlerList;
			exceptionHandlerList.Rule = MakePlusRule(exceptionHandlerList, exceptionHandler);
			exceptionHandler.Rule = Key("WHEN") + handledExceptions + Key("THEN") + plsqlStatementList;
			handledExceptions.Rule = Key("OTHERS") | exceptionNames;
			exceptionNames.Rule = MakePlusRule(exceptionNames, OR, Identifier);

			return plsqlBlock;
		}

		#region CREATE ...

		private NonTerminal Create() {
			var createCommand = new NonTerminal("create_command");

			// -- CREATE
			createCommand.Rule = CreateSchema() |
								  CreateTable() |
								  CreateView() |
								  CreateIndex() |
								  CreateSequence() |
								  CreateTrigger() |
								  CreateUser() |
								  CreateType();

			return createCommand;
		}

		private NonTerminal CreateTable() {
			var createTable = new NonTerminal("create_table", typeof(CreateTableNode));
			var ifNotExistsOpt = new NonTerminal("if_not_exists_opt");
			var temporaryOpt = new NonTerminal("temporary_opt");
			var columnOrConstraintList = new NonTerminal("column_or_constraint_list");
			var columnOrConstraint = new NonTerminal("column_or_constraint");
			var tableColumn = new NonTerminal("table_column", typeof (TableColumnNode));
			var columnDefaultOrIdentityOpt = new NonTerminal("column_default_or_identity_opt");
			var columnConstraintList = new NonTerminal("column_constraint_list");
			var columnConstraint = new NonTerminal("column_constraint", typeof (ColumnConstraintNode));
			var columnConstraintRefOpt = new NonTerminal("column_constraint_ref_opt");
			var fkeyActionList = new NonTerminal("fkey_action_list");
			var fkeyAction = new NonTerminal("fkey_action");
			var fkeyActionType = new NonTerminal("fkey_action_type");
			var tableConstraint = new NonTerminal("table_constraint", typeof (TableConstraintNode));
			var constraintName = new NonTerminal("constraint_name");
			var tableConstraintNameOpt = new NonTerminal("table_constraint_name_opt");
			var defTableConstraint = new NonTerminal("def_table_constraint");
			var columnList = new NonTerminal("column_list");

			createTable.Rule = CREATE + temporaryOpt + TABLE + ifNotExistsOpt + ObjectName() + "(" + columnOrConstraintList + ")";
			ifNotExistsOpt.Rule = Empty | IF + NOT + EXISTS;
			temporaryOpt.Rule = Empty | Key("TEMPORARY");
			columnOrConstraintList.Rule = MakePlusRule(columnOrConstraintList, Comma, columnOrConstraint);

			columnOrConstraint.Rule = tableColumn | tableConstraint;

			tableColumn.Rule = Identifier + DataType() + columnConstraintList + columnDefaultOrIdentityOpt;

			columnConstraintList.Rule = MakeStarRule(columnConstraintList, columnConstraint);
			columnConstraint.Rule = Key("NULL") |
			                         Key("NOT") + Key("NULL") |
			                         Key("UNIQUE") |
			                         Key("PRIMARY") + Key("KEY") |
			                         Key("CHECK") + SqlExpression() |
			                         Key("REFERENCES") + ObjectName() + columnConstraintRefOpt + fkeyActionList;
			columnConstraintRefOpt.Rule = Empty | "(" + Identifier + ")";
			columnDefaultOrIdentityOpt.Rule = Empty | Key("DEFAULT") + SqlExpression() | Key("IDENTITY");
			fkeyActionList.Rule = MakeStarRule(fkeyActionList, fkeyAction);
			fkeyAction.Rule = ON + DELETE + fkeyActionType | ON + UPDATE + fkeyActionType;
			fkeyActionType.Rule = Key("CASCADE") |
			                      Key("SET") + Key("NULL") |
			                      Key("SET") + Key("DEFAULT") |
			                      Key("NO") + Key("ACTION");

			tableConstraint.Rule = tableConstraintNameOpt + defTableConstraint;
			tableConstraintNameOpt.Rule = Empty | CONSTRAINT + constraintName;
			constraintName.Rule = Identifier;
			defTableConstraint.Rule = PRIMARY + KEY + "(" + columnList + ")" |
			                            UNIQUE + "(" + columnList + ")" |
			                            CHECK + "(" + SqlExpression() + ")" |
			                            FOREIGN + KEY + "(" + columnList + ")" + REFERENCES + ObjectName() + "(" + columnList +
			                            ")" +
			                            fkeyActionList;
			columnList.Rule = MakePlusRule(columnList, Comma, Identifier);

			return createTable;
		}

		private NonTerminal CreateView() {
			var createView = new NonTerminal("create_view", typeof(CreateViewNode));
			var columnList = new NonTerminal("column_list");
			var columnName = new NonTerminal("column_name");
			var columnListOpt = new NonTerminal("column_list_opt");

			columnName.Rule = Identifier;
			columnList.Rule = MakeStarRule(columnList, Comma, columnName);
			columnListOpt.Rule = Empty | "(" + columnList + ")";
			createView.Rule = CREATE + OrReplace() + VIEW + ObjectName() + columnListOpt + "AS" + SqlQueryExpression();
			return createView;
		}

		private NonTerminal CreateUser() {
			var createUser = new NonTerminal("create_user", typeof (CreateUserStatementNode));

			var identifiedRule = new NonTerminal("identified");
			var identifiedByPassword = new NonTerminal("identified_by_password", typeof (IdentifiedByPasswordNode));
			var identifiedByAuth = new NonTerminal("identified_by_auth");
			var setAccountLockOpt = new NonTerminal("set_account_lock_opt");
			var setGroupsOpt = new NonTerminal("set_groups_opt");

			createUser.Rule = Key("CREATE") + Key("USER") + Identifier +
			                  Key("IDENTIFIED") + identifiedRule + setAccountLockOpt + setGroupsOpt;
			identifiedRule.Rule = identifiedByPassword | identifiedByAuth;
			identifiedByPassword.Rule = Key("BY") + Key("PASSWORD") + SqlExpression();
			identifiedByAuth.Rule = Key("BY") + StringLiteral;

			setAccountLockOpt.Rule = Empty |
			                         SET + ACCOUNT + LOCK |
			                         SET + ACCOUNT + UNLOCK;
			setGroupsOpt.Rule = Empty | SET + GROUPS + StringLiteral;

			return createUser;
		}

		private NonTerminal CreateIndex() {
			var createIndex = new NonTerminal("create_index");

			var columnList = new NonTerminal("column_list");
			var columnName = new NonTerminal("column_name");

			columnName.Rule = Identifier;
			columnList.Rule = MakePlusRule(columnList, Comma, columnName);

			createIndex.Rule = CREATE + INDEX + ObjectName() + ON + ObjectName() + "(" + columnList + ")";

			return createIndex;
		}

		private NonTerminal CreateSequence() {
			var createSequence = new NonTerminal("create_sequence", typeof(CreateSequenceNode));

			var incrementOpt = new NonTerminal("increment_opt");
			var increment = new NonTerminal("increment");
			var startOpt = new NonTerminal("start_opt");
			var start = new NonTerminal("start");
			var minvalueOpt = new NonTerminal("minvalue_opt");
			var minvalue = new NonTerminal("minvalue");
			var maxvalueOpt = new NonTerminal("maxvalue_opt");
			var maxvalue = new NonTerminal("maxvalue");
			var cacheOpt = new NonTerminal("cache_opt");
			var cycleOpt = new NonTerminal("cycle_opt");
			var cache = new NonTerminal("cache");

			createSequence.Rule = Key("CREATE") + Key("SEQUENCE") + ObjectName() +
			                       incrementOpt +
			                       startOpt +
			                       minvalueOpt +
			                       maxvalueOpt +
			                       cacheOpt +
			                       cycleOpt;
			incrementOpt.Rule = Empty | increment;
			increment.Rule = Key("INCREMENT") + Key("BY") + SqlExpression();
			startOpt.Rule = Empty | start;
			start.Rule = Key("START") + Key("WITH") + SqlExpression();
			minvalueOpt.Rule = Empty | minvalue;
			minvalue.Rule = Key("MINVALUE") + SqlExpression();
			maxvalueOpt.Rule = Empty | maxvalue;
			maxvalue.Rule = Key("MAXVALUE") + SqlExpression();
			cycleOpt.Rule = Empty | Key("CYCLE");
			cacheOpt.Rule = Empty | cache;
			cache.Rule = Key("CACHE") + SqlExpression();

			return createSequence;
		}

		private NonTerminal CreateSchema() {
			var createSchema = new NonTerminal("create_schema", typeof(CreateSchemaNode));
			createSchema.Rule = Key("CREATE") + Key("SCHEMA") + Identifier;

			return createSchema;
		}

		private NonTerminal CreateTrigger() {
			var createTrigger = new NonTerminal("create_trigger", typeof(CreateTriggerNode));
			var createProcedureTrigger = new NonTerminal("create_procedure_trigger");
			var createCallbackTrigger = new NonTerminal("create_callback_trigger");
			var triggerSync = new NonTerminal("trigger_sync");
			var beforeOrAfter = new NonTerminal("before_or_after");
			var triggerEvents = new NonTerminal("trigger_events");
			var triggerEvent = new NonTerminal("trigger_event");
			var triggerBody = new NonTerminal("trigger_body");
			var triggerRefOpt = new NonTerminal("trigger_ref_opt");
			var triggerRefList = new NonTerminal("trigger_ref_list");
			var triggerRef = new NonTerminal("trigger_ref");
			var stateTableRef = new NonTerminal("state_table_ref");
			var asOpt = new NonTerminal("as_opt");

			var functionCallArgsOpt = new NonTerminal("function_call_args_opt");
			var functionCallArgsList = new NonTerminal("function_call_args_list");

			createTrigger.Rule = createProcedureTrigger | createCallbackTrigger;
			createCallbackTrigger.Rule = CREATE + OrReplace() + CALLBACK + TRIGGER +
			                               beforeOrAfter + ON + ObjectName();
			createProcedureTrigger.Rule = CREATE + OrReplace() + TRIGGER + ObjectName() +
			                                triggerSync + triggerRefOpt +
			                                FOR + EACH + ROW + triggerBody;
			triggerSync.Rule = beforeOrAfter + triggerEvents + ON + ObjectName();
			beforeOrAfter.Rule = BEFORE | AFTER;
			triggerEvents.Rule = MakePlusRule(triggerEvents, OR, triggerEvent);
			triggerEvent.Rule = INSERT | UPDATE | DELETE;
			triggerRefOpt.Rule = Empty | Key("REFERENCING") + triggerRefList;
			triggerRefList.Rule = MakePlusRule(triggerRefList, triggerRef);
			triggerRef.Rule = stateTableRef + asOpt + Identifier;
			asOpt.Rule = Empty | Key("AS");
			stateTableRef.Rule = Key("OLD") | Key("NEW");
			triggerBody.Rule = EXECUTE + PROCEDURE + ObjectName() + "(" + functionCallArgsList + ")" |
			                    PlSqlBlock();

			functionCallArgsOpt.Rule = Empty | "(" + functionCallArgsList + ")";
			functionCallArgsList.Rule = MakeStarRule(functionCallArgsList, Comma, SqlExpression());

			return createTrigger;
		}

		private NonTerminal CreateType() {
			var createType = new NonTerminal("create_type", typeof(CreateTypeNode));
			var typeAttrs = new NonTerminal("type_attributes");
			var typeAttr = new NonTerminal("type_attribute", typeof(TypeAttributeNode));
			var orReplaceOpt = new NonTerminal("or_replace");

			createType.Rule = Key("CREATE") + orReplaceOpt + Key("TYPE") + ObjectName() + "(" + typeAttrs + ")";
			orReplaceOpt.Rule = Empty | Key("OR") + Key("REPLACE");
			typeAttrs.Rule = MakePlusRule(typeAttrs, Comma, typeAttr);
			typeAttr.Rule = Identifier + DataType();

			return createType;
		}

		#endregion

		#region ALTER ...

		private NonTerminal Alter() {
			var alterCommand = new NonTerminal("alter_command");

			alterCommand.Rule = AlterTable() | AlterUser();

			return alterCommand;
		}

		private NonTerminal AlterTable() {
			var alterTable = new NonTerminal("alter_table", typeof(AlterTableNode));
			var alterActions = new NonTerminal("alter_actions");
			var alterAction = new NonTerminal("alter_action");
			var addColumn = new NonTerminal("add_column", typeof(AddColumnNode));
			var columnOpt = new NonTerminal("column_opt");
			var addConstraint = new NonTerminal("add_constraint", typeof(AddConstraintNode));
			var dropColumn = new NonTerminal("drop_column", typeof(DropColumnNode));
			var dropConstraint = new NonTerminal("drop_constraint", typeof(DropConstraintNode));
			var dropPrimaryKey = new NonTerminal("drop_primary_key", typeof(DropPrimaryKeyNode));
			var setDefault = new NonTerminal("set_default", typeof(SetDefaultNode));
			var alterColumn = new NonTerminal("alter_column", typeof(AlterColumnNode));
			var dropDefault = new NonTerminal("drop_default", typeof(DropDefaultNode));
			var columnDef = new NonTerminal("column_def", typeof(TableColumnNode));
			var columnConstraintList = new NonTerminal("column_constraint_list");
			var columnDefaultOpt = new NonTerminal("column_default_opt");
			var columnIdentityOpt = new NonTerminal("column_identity_opt");
			var columnConstraint = new NonTerminal("column_constraint", typeof(ColumnConstraintNode));
			var columnConstraintRefOpt = new NonTerminal("column_constraint_ref_opt");
			var fkeyActionList = new NonTerminal("fkey_action_list");
			var fkeyAction = new NonTerminal("fkey_action");
			var fkeyActionType = new NonTerminal("fkey_action_type");
			var tableConstraint = new NonTerminal("table_constraint", typeof(TableConstraintNode));
			var constraintName = new NonTerminal("constraint_name");
			var tableConstraintNameOpt = new NonTerminal("table_constraint_name_opt");
			var defTableConstraint = new NonTerminal("def_table_constraint");
			var columnList = new NonTerminal("column_list");

			alterTable.Rule = Key("ALTER") + Key("TABLE") + ObjectName() + alterActions;
			alterActions.Rule = MakePlusRule(alterActions, alterAction);
			alterAction.Rule = addColumn | addConstraint | dropColumn | dropConstraint | dropPrimaryKey | setDefault | dropDefault | alterColumn;
			addColumn.Rule = Key("ADD") + columnOpt + columnDef;
			columnOpt.Rule = Empty | Key("COLUMN");
			dropColumn.Rule = Key("DROP") + columnOpt + Identifier;
			addConstraint.Rule = Key("ADD") + Key("CONSTRAINT") + tableConstraint;
			dropConstraint.Rule = Key("DROP") + Key("CONSTRAINT") + Identifier;
			dropPrimaryKey.Rule = Key("DROP") + Key("PRIMARY") + Key("KEY");
			dropDefault.Rule = Key("ALTER") + columnOpt + Identifier + Key("DROP") + Key("DEFAULT");
			setDefault.Rule = Key("ALTER") + columnOpt + Identifier + Key("SET") + Key("DEFAULT") + SqlExpression();
			alterColumn.Rule = Key("ALTER") + columnOpt + columnDef;
			columnDef.Rule = Identifier + DataType() + columnConstraintList + columnDefaultOpt + columnIdentityOpt;
			columnConstraintList.Rule = MakeStarRule(columnConstraintList, columnConstraint);
			columnConstraint.Rule = Key("NULL") |
									 Key("NOT") + Key("NULL") |
									 Key("UNIQUE") |
									 Key("PRIMARY") + Key("KEY") |
									 Key("CHECK") + SqlExpression() |
									 Key("REFERENCES") + ObjectName() + columnConstraintRefOpt + fkeyActionList;
			columnConstraintRefOpt.Rule = Empty | "(" + Identifier + ")";
			columnDefaultOpt.Rule = Empty | Key("DEFAULT") + SqlExpression();
			columnIdentityOpt.Rule = Empty | Key("IDENTITY");
			fkeyActionList.Rule = MakeStarRule(fkeyActionList, fkeyAction);
			fkeyAction.Rule = ON + DELETE + fkeyActionType | ON + UPDATE + fkeyActionType;
			fkeyActionType.Rule = Key("CASCADE") |
								  Key("SET") + Key("NULL") |
								  Key("SET") + Key("DEFAULT") |
								  Key("NO") + Key("ACTION");

			tableConstraint.Rule = tableConstraintNameOpt + defTableConstraint;
			tableConstraintNameOpt.Rule = Empty | constraintName;
			constraintName.Rule = Identifier;
			defTableConstraint.Rule = Key("PRIMARY") + Key("KEY") + "(" + columnList + ")" |
										Key("UNIQUE") + "(" + columnList + ")" |
										Key("CHECK") + "(" + SqlExpression() + ")" |
										FOREIGN + KEY + "(" + columnList + ")" + REFERENCES + ObjectName() + "(" + columnList +
										")" +
										fkeyActionList;
			columnList.Rule = MakePlusRule(columnList, Comma, Identifier);

			return alterTable;
		}

		private NonTerminal AlterUser() {
			var alterUser = new NonTerminal("alter_user", typeof(AlterUserStatementNode));
			var actionList = new NonTerminal("action_list");
			var action = new NonTerminal("action");
			var setAccountStatus = new NonTerminal("set_account_status", typeof(SetAccountStatusNode));
			var setPassword = new NonTerminal("set_password", typeof(SetPasswordNode));
			var setGroups = new NonTerminal("set_groups", typeof(SetGroupsNode));
			var accountStatus = new NonTerminal("account_status");

			alterUser.Rule = Key("ALTER") + Key("USER") + Identifier + actionList;
			actionList.Rule = MakePlusRule(actionList, action);
			action.Rule = setAccountStatus | setPassword | setGroups;
			setAccountStatus.Rule = Key("SET") + Key("ACCOUNT") + accountStatus;
			accountStatus.Rule = Key("LOCK") | Key("UNLOCK");
			setPassword.Rule = Key("SET") + Key("PASSWORD") + SqlExpression();
			setGroups.Rule = Key("SET") + Key("GROUPS") + SqlExpressionList();

			return alterUser;
		}

		#endregion

		private NonTerminal Drop() {
			var dropCommand = new NonTerminal("drop_command");

			dropCommand.Rule = DropSchema() |
			                   DropTable() |
			                   DropView() |
			                   DropIndex() |
			                   DropSequence() |
			                   DropTrigger() |
			                   DropUser() |
			                   DropType();

			return dropCommand;
		}

		private NonTerminal DropSchema() {
			var dropSchema = new NonTerminal("drop_schema", typeof(DropSchemaStatementNode));

			dropSchema.Rule = Key("DROP") + Key("SCHEMA") + Identifier;
			return dropSchema;
		}

		private NonTerminal DropTable() {
			var dropTable = new NonTerminal("drop_table", typeof(DropTableStatementNode));
			var tableNameList = new NonTerminal("table_name_list");
			var ifExistsOpt = new NonTerminal("if_exists_opt");

			dropTable.Rule = Key("DROP") + Key("TABLE") + ifExistsOpt + tableNameList;
			tableNameList.Rule = MakePlusRule(tableNameList, Comma, ObjectName());
			ifExistsOpt.Rule = Empty | Key("IF") + Key("EXISTS");

			return dropTable;
		}

		private NonTerminal DropView() {
			var dropView = new NonTerminal("drop_view", typeof(DropViewStatementNode));
			var viewNameList = new NonTerminal("view_name_list");
			var ifExistsOpt = new NonTerminal("if_exists_opt");

			dropView.Rule = Key("DROP") + Key("VIEW") + ifExistsOpt + viewNameList;
			viewNameList.Rule = MakePlusRule(viewNameList, Comma, ObjectName());
			ifExistsOpt.Rule = Empty | Key("IF") + Key("EXISTS");

			return dropView;
		}

		private NonTerminal DropIndex() {
			var dropIndex = new NonTerminal("drop_index");
			dropIndex.Rule = Key("DROP") + Key("INDEX") + ObjectName();
			return dropIndex;
		}

		private NonTerminal DropSequence() {
			var dropSequence = new NonTerminal("drop_index");
			dropSequence.Rule = Key("DROP") + Key("SEQUENCE") + ObjectName();
			return dropSequence;
		}

		private NonTerminal DropTrigger() {
			var dropTrigger = new NonTerminal("drop_trigger", typeof(DropTriggerStatementNode));
			var dropProcedureTrigger = new NonTerminal("drop_procedure_trigger");
			var dropCallbackTrigger = new NonTerminal("drop_callback_trigger");

			dropTrigger.Rule = dropProcedureTrigger | dropCallbackTrigger;
			dropProcedureTrigger.Rule = Key("DROP") + Key("TRIGGER") + ObjectName();
			dropCallbackTrigger.Rule = Key("DROP") + Key("CALLBACK") + Key("TRIGGER") + Key("FROM") + ObjectName();
			return dropTrigger;
		}

		private NonTerminal DropUser() {
			var dropUser = new NonTerminal("drop_user", typeof(DropUserStatementNode));
			var userNameList = new NonTerminal("user_list");

			dropUser.Rule = Key("DROP") + Key("USER") + userNameList;
			userNameList.Rule = MakePlusRule(userNameList, Comma, Identifier);
			return dropUser;
		}

		private NonTerminal DropType() {
			var dropType = new NonTerminal("drop_type", typeof(DropTypeStatementNode));
			dropType.Rule = Key("DROP") + Key("TYPE") + ObjectName();
			return dropType;
		}

		private NonTerminal Commit() {
			var commitCommand = new NonTerminal("commit", typeof(CommitStatementNode));
			commitCommand.Rule = Key("COMMIT");
			return commitCommand;
		}

		private NonTerminal Rollback() {
			var command = new NonTerminal("rollback", typeof(RollbackStatementNode));
			command.Rule = Key("ROLLBACK");
			return command;
		}

		private NonTerminal Select() {
			var selectCommand = new NonTerminal("select_command", typeof(SelectStatementNode));
			var orderOpt = new NonTerminal("order_opt");
			var sortedDef = new NonTerminal("sorted_def", typeof(OrderByNode));
			var sortedDefList = new NonTerminal("sorted_def_list");
			var sortOrder = new NonTerminal("sort_order");
			var limitOpt = new NonTerminal("limit_opt");
			var limit = new NonTerminal("limit", typeof(LimitNode));

			selectCommand.Rule = SqlQueryExpression() + orderOpt + limitOpt;

			orderOpt.Rule = Empty | Key("ORDER") + Key("BY") + sortedDefList;
			sortedDef.Rule = SqlExpression() + sortOrder;
			sortOrder.Rule = Key("ASC") | Key("DESC");
			sortedDefList.Rule = MakePlusRule(sortedDefList, Comma, sortedDef);

			limitOpt.Rule = Empty | limit;
			limit.Rule = Key("LIMIT") + PositiveLiteral + "," + PositiveLiteral |
			             Key("LIMIT") + PositiveLiteral;

			return selectCommand;
		}

		private NonTerminal Grant() {
			var grant = new NonTerminal("grant");
			var grantObject = new NonTerminal("grant_object", typeof(GrantStatementNode));
			var grantPriv = new NonTerminal("grant_priv", typeof(GrantRoleStatementNode));
			var roleList = new NonTerminal("role_list");
			var priv = new NonTerminal("priv", typeof(PrivilegeNode));
			var privList = new NonTerminal("priv_list");
			var objPriv = new NonTerminal("object_priv");
			var privilegeOpt = new NonTerminal("privilege_opt");
			var privilegesOpt = new NonTerminal("privileges_opt");
			var distributionList = new NonTerminal("distribution_list");
			var withAdminOpt = new NonTerminal("with_admin_opt");
			var withAdmin = new NonTerminal("with_admin");
			var withGrantOpt = new NonTerminal("with_grant_opt");
			var withGrant = new NonTerminal("with_grant");
			var optionOpt = new NonTerminal("option_opt");
			var columnList = new NonTerminal("column_list");
			var columnListOpt = new NonTerminal("column_list_opt");
			var referencePriv = new NonTerminal("reference_priv");
			var updatePriv = new NonTerminal("update_priv");
			var selectPriv = new NonTerminal("select_priv");

			grant.Rule = grantObject | grantPriv;
			grantPriv.Rule = Key("GRANT") + roleList + Key("TO") + distributionList + withAdminOpt;
			roleList.Rule = MakePlusRule(roleList, Comma, Identifier);
			withAdminOpt.Rule = Empty | withAdmin;
			withAdmin.Rule = Key("WITH") + Key("ADMIN") + optionOpt;
            optionOpt.Rule = Empty | Key("OPTION");

			grantObject.Rule = Key("GRANT") + objPriv + Key("ON") + ObjectName() + Key("TO") + distributionList + withGrantOpt;
			objPriv.Rule = Key("ALL") + privilegesOpt | privList;
			privilegesOpt.Rule = Empty | Key("PRIVILEGES");
			privilegeOpt.Rule = Empty | Key("PRIVILEGE");
			privList.Rule = MakePlusRule(privList, Comma, priv);
			priv.Rule = Key("USAGE") + privilegeOpt|
						Key("INSERT") + privilegeOpt |
						Key("DELETE") + privilegeOpt |
						Key("EXECUTE") + privilegeOpt |
						Key("ALTER") + privilegeOpt |
						Key("INDEX") + privilegeOpt |
						updatePriv |
						referencePriv |
						selectPriv;
			updatePriv.Rule = Key("UPDATE") + privilegeOpt + columnListOpt;
			referencePriv.Rule = Key("REFERENCES") + privilegeOpt + columnListOpt;
			selectPriv.Rule = Key("SELECT") + columnListOpt;
			columnListOpt.Rule = Empty | "(" + columnList + ")";
			columnList.Rule = MakePlusRule(columnList, Comma, Identifier);
			withGrantOpt.Rule = Empty | withGrant;
			withGrant.Rule = Key("WITH") + Key("GRANT") + optionOpt;
			distributionList.Rule = MakePlusRule(distributionList, Comma, Identifier);

			return grant;
		}

		private NonTerminal Revoke() {
			var revoke = new NonTerminal("revoke");
			var grantObject = new NonTerminal("grant_object");
			var grantPriv = new NonTerminal("grant_priv");
			var roleList = new NonTerminal("role_list");
			var priv = new NonTerminal("priv");
			var privList = new NonTerminal("priv_list");
			var objPriv = new NonTerminal("object_priv");
			var privilegeOpt = new NonTerminal("privilege_opt");
			var privilegesOpt = new NonTerminal("privileges_opt");
			var distributionList = new NonTerminal("distribution_list");

			revoke.Rule = grantObject | grantPriv;
			grantPriv.Rule = Key("REVOKE") + roleList + Key("FROM") + distributionList;
			roleList.Rule = MakePlusRule(roleList, Comma, Identifier);

			grantObject.Rule = Key("REVOKE") + objPriv + Key("ON") + ObjectName() + Key("TO") + distributionList;
			objPriv.Rule = Key("ALL") + privilegesOpt | privList;
			privilegesOpt.Rule = Empty | Key("PRIVILEGES");
			privilegeOpt.Rule = Empty | Key("PRIVILEGE");
			privList.Rule = MakePlusRule(privList, Comma, priv);
			priv.Rule = Key("USAGE") + privilegeOpt |
			            Key("INSERT") + privilegeOpt |
			            Key("DELETE") + privilegeOpt |
			            Key("EXECUTE") + privilegeOpt |
			            Key("ALTER") + privilegeOpt |
			            Key("INDEX") + privilegeOpt |
			            Key("UPDATE") + privilegeOpt |
			            Key("REFERENCES") + privilegeOpt |
			            Key("SELECT") + privilegeOpt;
			distributionList.Rule = MakePlusRule(distributionList, Comma, Identifier);

			return revoke;
		}

		private NonTerminal Insert() {
			var insert = new NonTerminal("insert_command", typeof(InsertStatementNode));
			var insertSource = new NonTerminal("insert_source");
			var sourceWithColumns = new NonTerminal("source_with_columns");
			var fromValues = new NonTerminal("from_values", typeof(ValuesInsertNode));

			var fromQuery = new NonTerminal("from_query", typeof(QueryInsertNode));
			var fromSet = new NonTerminal("from_set", typeof(SetInsertNode));
			var columnList = new NonTerminal("column_list");
			var columnListOpt = new NonTerminal("column_list_opt");
			var columnSet = new NonTerminal("column_set", typeof(InsertSetNode));
			var columnSetList = new NonTerminal("column_set_list");
			var insertTuple = new NonTerminal("insert_tuple");
			var insertValue = new NonTerminal("insert_value", typeof(InsertValueNode));

			insert.Rule = Key("INSERT") + Key("INTO") + ObjectName() + insertSource;
			insertSource.Rule = columnListOpt + sourceWithColumns | fromSet;
			sourceWithColumns.Rule = fromQuery | fromValues;
			fromValues.Rule = columnListOpt + Key("VALUES") + insertTuple;
			fromQuery.Rule = columnListOpt + Key("FROM") + "(" + SqlQueryExpression() + ")";
			fromSet.Rule = Key("SET") +  columnSetList;
			columnListOpt.Rule = Empty | "(" + columnList + ")";
			columnList.Rule = MakePlusRule(columnList, Comma, Identifier);
			fromValues.Rule = Key("VALUES") + insertTuple;
			insertTuple.Rule = MakePlusRule(insertTuple, Comma, insertValue);
			insertValue.Rule = "(" + SqlExpressionList() + ")";
			columnSetList.Rule = MakePlusRule(columnSetList, Comma, columnSet);
			columnSet.Rule = Identifier + "=" + SqlExpression();
			return insert;
		}

		private NonTerminal Delete() {
			var deleteCommand = new NonTerminal("delete_command", typeof(DeleteStatementNode));
			var whereOpt = new NonTerminal("where_opt");
			var cursorDef = new NonTerminal("cursor_def");

			deleteCommand.Rule = Key("DELETE") + Key("FROM") + ObjectName() + whereOpt;
			whereOpt.Rule = Empty | Key("WHERE") + SqlQueryExpression();

			return deleteCommand;
		}

		private NonTerminal Update() {
			var update = new NonTerminal("update_command", typeof(UpdateStatementNode));
			var updateSimple = new NonTerminal("update_simple", typeof(SimpleUpdateNode));
			var updateQuery = new NonTerminal("update_query");
			var columnSet = new NonTerminal("column_set", typeof(UpdateColumnNode));
			var columnSetList = new NonTerminal("column_set_list");
			var columnList = new NonTerminal("column_list");
			var updateWhere = new NonTerminal("update_where");
			var limitOpt = new NonTerminal("limit_opt");

			update.Rule = updateSimple | updateQuery;
			updateSimple.Rule = Key("UPDATE") + ObjectName() + Key("SET") + columnSetList + updateWhere + limitOpt;
			updateQuery.Rule = Key("UPDATE") + ObjectName() + Key("SET") + "(" + columnList + ")" + "=" + SqlQueryExpression() + updateWhere + limitOpt;
			columnSetList.Rule = MakePlusRule(columnSetList, Comma, columnSet);
			columnSet.Rule = Identifier + "=" + SqlExpression();
			columnList.Rule = MakePlusRule(columnList, Comma, Identifier);
			updateWhere.Rule = Key("WHERE") + SqlExpression();
			limitOpt.Rule = Empty | Key("LIMIT") + PositiveLiteral;

			return update;
		}

		private NonTerminal Truncate() {
			var truncate = new NonTerminal("truncate_command");
			truncate.Rule = Key("TRUNCATE") + ObjectName();
			return truncate;
		}

		private NonTerminal Set() {
			var set = new NonTerminal("set_command");
			set.Rule = SetTransaction() |
			           SetVariable();
			return set;
		}

		private NonTerminal SetTransaction() {
			var set = new NonTerminal("set_transaction");
			var access = new NonTerminal("access");
			var accessType = new NonTerminal("access_type");
			var isolationLevel = new NonTerminal("isolation_level");
			var levelType = new NonTerminal("level_type");
			var defaultSchema = new NonTerminal("default_schema");

			set.Rule = access | isolationLevel | defaultSchema;
			access.Rule = Key("SET") + Key("TRANSACTION") + accessType;
			accessType.Rule = Key("READ") + Key("ONLY") | Key("READ") + Key("WRITE");
			isolationLevel.Rule = Key("SET") + Key("TRANSACTION") + Key("ISOLATION") + Key("LEVEL") + levelType;
			levelType.Rule = Key("SERIALIZABLE") |
			                 Key("READ") + Key("COMMITTED") |
			                 Key("READ") + Key("UNCOMMITTED") |
			                 Key("SNAPSHOT");
			defaultSchema.Rule = Key("SET") + Key("TRANSACTION") + Key("DEFAULT") + Key("SCHEMA") + Identifier;
			return set;
		}

		private NonTerminal SetVariable() {
			var varSet = new NonTerminal("var_set", typeof(SetVariableStatementNode));

			varSet.Rule = SqlExpression() + ":=" + SqlExpression();
			return varSet;
		}
	}
}