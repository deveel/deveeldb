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

using Deveel.Data.Sql.Statements;

using Irony.Parsing;

namespace Deveel.Data.Sql.Parser {
	partial class SqlGrammar : SqlGrammarBase {
		public override string Dialect {
			get { return "SQL-92"; }
		}

		protected KeyTerm Semicolon { get; private set; }

		protected override NonTerminal MakeRoot() {
			Semicolon = ToTerm(".");

			var root = new NonTerminal("root");
			Productions(root);

			return root;
		}

		private void Productions(NonTerminal root) {
			// PL/SQL
			var block = PlSqlBlock();
			var blockList = new NonTerminal("block_list");
			blockList.Rule = MakePlusRule(blockList, Semicolon, block);

			root.Rule = SqlStatementList() + Eof | 
				blockList + Eof;
		}

		private NonTerminal OrReplace() {
			var orReplaceOpt = new NonTerminal("or_replace_opt");
			orReplaceOpt.Rule = Empty | OR + REPLACE;
			return orReplaceOpt;
		}

		private NonTerminal SqlStatementList() {
			var commandList = new NonTerminal("command_list", typeof(SequenceOfStatementsNode));

			commandList.Rule = MakePlusRule(commandList, Semicolon, SqlStatement());

			return commandList;
		}

		private NonTerminal SqlStatement() {
			var sqlStatement = new NonTerminal("sql_statement");

			var toDefineData = new NonTerminal("to_define_data");
			var toControlData = new NonTerminal("to_control_data");
			var toModifyData = new NonTerminal("to_modify_data");

			sqlStatement.Rule = toDefineData |
			                    toControlData |
			                    toModifyData;

			toDefineData.Rule = Create() |
								Alter() |
			                    Drop();

			toModifyData.Rule = Select() |
			                    Insert() |
			                    Update() |
			                    Delete() |
			                    Truncate() |
								Set();

			toControlData.Rule = Grant() |
			                     Revoke();

			return sqlStatement;
		}

		private NonTerminal PlSqlBlock() {
			var plsqlBlock = new NonTerminal("plsql_block");
			var labelStatement = new NonTerminal("plsql_label");
			var labelStatementOpt = new NonTerminal("plsql_label_opt");
			var declareStatementOpt = new NonTerminal("declare_statement_opt");
			var declareSpec = new NonTerminal("declare_spec");
			var declareSpecList = new NonTerminal("declare_spec_list");
			var declare_variable = new NonTerminal("declare_variable");
			var constant_opt = new NonTerminal("constant_opt");
			var var_not_null_opt = new NonTerminal("var_not_null_opt");
			var var_default_opt = new NonTerminal("var_default_opt");
			var var_default_assign = new NonTerminal("var_default_assign");
			var plsql_code_block = new NonTerminal("plsql_code_block");
			var plsql_statement_list = new NonTerminal("plsql_statement_list");
			var plsql_statement = new NonTerminal("plsql_statement");
			var plsql_sql_statement = new NonTerminal("plsql_sql_statement");
			var exit_statement = new NonTerminal("exit_statement");
			var label_opt = new NonTerminal("label_opt");
			var exit_when_opt = new NonTerminal("exit_when_opt");
			var goto_statement = new NonTerminal("goto_statement");
			var conditional_statement = new NonTerminal("conditional_statement");
			var conditional_elsif_list = new NonTerminal("conditional_elsif_list");
			var conditional_elsif = new NonTerminal("conditional_elsif");
			var conditional_else_opt = new NonTerminal("conditional_else_opt");
			var declare_exception = new NonTerminal("declare_exception");
			var declare_pragma = new NonTerminal("declare_pragma");
			var declare_cursor = new NonTerminal("declare_cursor");
			var cursor_args_opt = new NonTerminal("cursor_args_opt");

			plsqlBlock.Rule = labelStatementOpt + declareStatementOpt + plsql_code_block;
			labelStatementOpt.Rule = Empty | labelStatement;
			labelStatement.Rule = "<<" + Identifier + ">>";
			declareStatementOpt.Rule = Empty | DECLARE + declareSpecList;
			declareSpecList.Rule = MakePlusRule(declareSpecList, Semicolon, declareSpec);
			declareSpec.Rule = declare_variable | declare_exception | declare_pragma;
			declare_variable.Rule = Identifier + constant_opt + DataType() + var_not_null_opt + var_default_opt;
			constant_opt.Rule = Empty | CONSTANT;
			var_not_null_opt.Rule = Empty | NOT + NULL;
			var_default_opt.Rule = Empty | var_default_assign + SqlExpression();
			var_default_assign.Rule = ":=" | DEFAULT;
			declare_exception.Rule = Identifier + EXCEPTION;
			declare_pragma.Rule = PRAGMA + EXCEPTION_INIT + Identifier + "(" + StringLiteral + "," + PositiveLiteral + ")";
			declare_cursor.Rule = CURSOR + Identifier + cursor_args_opt + IS + SqlQueryExpression();

			plsql_code_block.Rule = BEGIN + plsql_statement_list + END;
			plsql_statement_list.Rule = MakePlusRule(plsql_statement_list, plsql_statement);
			plsql_statement.Rule = plsql_sql_statement |
			                       exit_statement |
			                       goto_statement |
			                       conditional_statement;

			plsql_sql_statement.Rule = Select();

			exit_statement.Rule = EXIT + label_opt + exit_when_opt;
			label_opt.Rule = Empty | Identifier;
			exit_when_opt.Rule = Empty | WHEN + SqlExpression();

			goto_statement.Rule = GOTO + Identifier;

			conditional_statement.Rule = IF + SqlExpression() + THEN + plsql_statement_list +
			                             conditional_elsif_list +
			                             conditional_else_opt +
			                             END + IF;
			conditional_elsif_list.Rule = MakeStarRule(conditional_elsif_list, conditional_elsif);
			conditional_elsif.Rule = ELSIF + SqlExpression() + THEN + plsql_statement_list;
			conditional_else_opt.Rule = Empty | ELSE + plsql_statement_list;

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
			var columnDefaultOpt = new NonTerminal("column_default_opt");
			var columnIdentityOpt = new NonTerminal("column_identity_opt");
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

			tableColumn.Rule = Identifier + DataType() + columnConstraintList + columnDefaultOpt + columnIdentityOpt;

			columnConstraintList.Rule = MakeStarRule(columnConstraintList, columnConstraint);
			columnConstraint.Rule = NULL |
			                         NOT + NULL |
			                         UNIQUE |
			                         PRIMARY + KEY |
			                         CHECK + SqlExpression() |
			                         REFERENCES + ObjectName() + columnConstraintRefOpt + fkeyActionList;
			columnConstraintRefOpt.Rule = Empty | "(" + Identifier + ")";
			columnDefaultOpt.Rule = Empty | DEFAULT + SqlExpression();
			columnIdentityOpt.Rule = Empty | IDENTITY;
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
			createView.Rule = CREATE + OrReplace() + VIEW + ObjectName() + columnListOpt + AS + SqlQueryExpression();
			return createView;
		}

		private NonTerminal CreateUser() {
			var createUser = new NonTerminal("create_user");

			var identifiedRule = new NonTerminal("identified");
			var setAccountLockOpt = new NonTerminal("set_account_lock_opt");
			var setGroupsOpt = new NonTerminal("set_groups_opt");

			createUser.Rule = CREATE + USER + Identifier + identifiedRule;
			identifiedRule.Rule = IDENTIFIED + BY + PASSWORD + StringLiteral + setAccountLockOpt + setGroupsOpt |
			                       IDENTIFIED + BY + StringLiteral + setAccountLockOpt + setGroupsOpt |
			                       IDENTIFIED + EXTERNALLY;
			setAccountLockOpt.Rule = SET + ACCOUNT + LOCK |
			                            SET + ACCOUNT + UNLOCK |
			                            Empty;
			setGroupsOpt.Rule = SET + GROUPS + StringLiteral | Empty;

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
			var createSequence = new NonTerminal("create_sequence");

			var incrementOpt = new NonTerminal("sequence_increment_opt");
			var startOpt = new NonTerminal("sequence_start_opt");
			var minvalueOpt = new NonTerminal("sequence_minvalue_opt");
			var maxvalueOpt = new NonTerminal("sequence_maxvalue_opt");
			var cacheOpt = new NonTerminal("sequence_cache_opt");
			var cycleOpt = new NonTerminal("sequence_cycle_opt");

			createSequence.Rule = CREATE + SEQUENCE + ObjectName() +
			                       incrementOpt +
			                       startOpt +
			                       minvalueOpt +
			                       maxvalueOpt +
			                       cacheOpt +
			                       cycleOpt;
			incrementOpt.Rule = INCREMENT + BY + SqlExpression() | Empty;
			startOpt.Rule = START + WITH + SqlExpression() | Empty;
			minvalueOpt.Rule = MINVALUE + SqlExpression() | Empty;
			maxvalueOpt.Rule = MAXVALUE + SqlExpression() | Empty;
			cycleOpt.Rule = CYCLE | Empty;
			cacheOpt.Rule = CACHE + SqlExpression() | Empty;

			return createSequence;
		}

		private NonTerminal CreateSchema() {
			var createSchema = new NonTerminal("create_schema");
			createSchema.Rule = CREATE + SCHEMA + Identifier;

			return createSchema;
		}

		private NonTerminal CreateTrigger() {
			var createTrigger = new NonTerminal("create_trigger", typeof(CreateTriggerNode));
			var createProcedureTrigger = new NonTerminal("create_procedure_trigger");
			var createCallbackTrigger = new NonTerminal("create_callback_trigger");
			var beforeOrAfter = new NonTerminal("before_or_after");
			var triggerEvents = new NonTerminal("trigger_events");
			var triggerEvent = new NonTerminal("trigger_event");
			var triggerBody = new NonTerminal("trigger_body");

			var functionCallArgsOpt = new NonTerminal("function_call_args_opt");
			var functionCallArgsList = new NonTerminal("function_call_args_list");

			createTrigger.Rule = createProcedureTrigger | createCallbackTrigger;
			createCallbackTrigger.Rule = CREATE + OrReplace() + CALLBACK + TRIGGER +
			                               beforeOrAfter + ON + ObjectName();
			createProcedureTrigger.Rule = CREATE + OrReplace() + TRIGGER + ObjectName() +
			                                beforeOrAfter + ON + ObjectName() +
			                                FOR + EACH + ROW;
			beforeOrAfter.Rule = BEFORE | AFTER;
			triggerEvents.Rule = MakePlusRule(triggerEvents, OR, triggerEvent);
			triggerEvent.Rule = INSERT | UPDATE | DELETE;
			triggerBody.Rule = EXECUTE + PROCEDURE + ObjectName() + "(" + functionCallArgsList + ")" |
			                    PlSqlBlock();

			functionCallArgsOpt.Rule = Empty | "(" + functionCallArgsList + ")";
			functionCallArgsList.Rule = MakeStarRule(functionCallArgsList, Comma, SqlExpression());

			return createTrigger;
		}

		private NonTerminal CreateType() {
			var createType = new NonTerminal("create_type");

			createType.Rule = CREATE + OrReplace() + TYPE + ObjectName();

			return createType;
		}

		#endregion

		#region ALTER ...

		private NonTerminal Alter() {
			var alterCommand = new NonTerminal("alter_command");

			alterCommand.Rule = AlterTable();

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
			var columnConstraintList = new NonTerminal("column_constraint_lst");
			var columnDefaultOpt = new NonTerminal("column_default_opt");
			var columnIdentityOpt = new NonTerminal("column_identity_opt");
			var columnConstraint = new NonTerminal("column_constraint");
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
			columnIdentityOpt.Rule = columnConstraintList.Rule = MakeStarRule(columnConstraintList, columnConstraint);
			columnConstraint.Rule = Key("NULL") |
									 Key("NOT") + Key("NULL") |
									 Key("UNIQUE") |
									 "PRIMARY" + "KEY" |
									 "CHECK" + SqlExpression() |
									 "REFERENCES" + ObjectName() + columnConstraintRefOpt + fkeyActionList;
			columnConstraintRefOpt.Rule = Empty | "(" + Identifier + ")";
			columnDefaultOpt.Rule = Empty | DEFAULT + SqlExpression();
			columnIdentityOpt.Rule = Empty | IDENTITY;
			columnIdentityOpt.Rule = Empty | IDENTITY;
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

			return alterTable;
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
			var dropSchema = new NonTerminal("drop_schema");
			dropSchema.Rule = Key("DROP") + Key("SCHEMA") + ObjectName();
			return dropSchema;
		}

		private NonTerminal DropTable() {
			var dropTable = new NonTerminal("drop_table");
			dropTable.Rule = Key("DROP") + Key("TABLE") + ObjectName();
			return dropTable;
		}

		private NonTerminal DropView() {
			var dropView = new NonTerminal("drop_view");
			dropView.Rule = Key("DROP") + Key("VIEW") + ObjectName();
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
			var dropTrigger = new NonTerminal("drop_trigger");
			var dropProcedureTrigger = new NonTerminal("drop_procedure_trigger");
			var dropCallbackTrigger = new NonTerminal("drop_callback_trigger");

			dropTrigger.Rule = dropProcedureTrigger | dropCallbackTrigger;
			dropProcedureTrigger.Rule = Key("DROP") + Key("TRIGGER") + ObjectName();
			dropCallbackTrigger.Rule = Key("DROP") + Key("CALLBACK") + Key("TRIGGER") + Key("FROM") + ObjectName();
			return dropTrigger;
		}

		private NonTerminal DropUser() {
			var dropUser = new NonTerminal("drop_user");
			dropUser.Rule = Key("DROP") + Key("USER") + Identifier;
			return dropUser;
		}

		private NonTerminal DropType() {
			var dropType = new NonTerminal("drop_type");
			dropType.Rule = Key("DROP") + Key("TYPE") + ObjectName();
			return dropType;
		}

		private NonTerminal Select() {
			var selectCommand = new NonTerminal("select_command", typeof(SelectStatementNode));
			var orderOpt = new NonTerminal("order_opt");
			var sortedDef = new NonTerminal("sorted_def", typeof(OrderByNode));
			var sortedDefList = new NonTerminal("sorted_def_list");
			var sortOrder = new NonTerminal("sort_order");

			selectCommand.Rule = SqlQueryExpression() + orderOpt;

			orderOpt.Rule = Empty | Key("ORDER") + Key("BY") + sortedDefList;
			sortedDef.Rule = SqlExpression() + sortOrder;
			sortOrder.Rule = Key("ASC") | Key("DESC");
			sortedDefList.Rule = MakePlusRule(sortedDefList, Comma, sortedDef);

			return selectCommand;
		}

		private NonTerminal Grant() {
			var grant = new NonTerminal("grant");
			var grantObject = new NonTerminal("grant_object");
			var grantPriv = new NonTerminal("grant_priv");
			var roleList = new NonTerminal("role_list");
			var priv = new NonTerminal("priv");
			var privList = new NonTerminal("priv_list");
			var objPriv = new NonTerminal("object_priv");
			var privilegeOpt = new NonTerminal("privilege_opt");
			var privilegesOpt = new NonTerminal("privileges_opt");
			var distributionList = new NonTerminal("distribution_list");
			var withAdminOpt = new NonTerminal("with_admin_opt");
			var withGrantOpt = new NonTerminal("with_grant_opt");
			var optionOpt = new NonTerminal("option_opt");
			var columnList = new NonTerminal("column_list");
			var columnListOpt = new NonTerminal("column_list_opt");
			var referencePriv = new NonTerminal("reference_priv");
			var updatePriv = new NonTerminal("update_priv");
			var selectPriv = new NonTerminal("select_priv");

			grant.Rule = grantObject | grantPriv;
			grantPriv.Rule = Key("GRANT") + roleList + Key("TO") + distributionList + withAdminOpt;
			roleList.Rule = MakePlusRule(roleList, Comma, Identifier);
			withAdminOpt.Rule = Empty | Key("WITH") + Key("ADMIN") + optionOpt;
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
			withGrantOpt.Rule = Key("WITH") + Key("GRANT") + optionOpt;
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
			var deleteCommand = new NonTerminal("delete_command");
			var whereOpt = new NonTerminal("where_opt");

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
			set.Rule = Key("SET");
			return set;
		}
	}
}