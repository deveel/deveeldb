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

namespace Deveel.Data.Sql.Compile {
	partial class SqlGrammar : Grammar {
		public SqlGrammar(bool ignoreCase)
			: base(!ignoreCase) {
			MakeSimpleId();

			Comments();
			Keywords();
			Literals();
			Productions();

			Operators();

			MarkPunctuation(",", "(", ")");
			MarkPunctuation(as_opt, select_as_opt, semicolon_opt);

			SetupRoot();
		}

		private KeyTerm comma;
		private KeyTerm dot;
		private KeyTerm colon;
		private KeyTerm semicolon;

		#region Literals

		private StringLiteral string_literal;
		private NumberLiteral number_literal;
		private NumberLiteral positive_literal;

		#endregion

		#region Non Terminals

		private IdentifierTerminal simple_id;

		private readonly NonTerminal object_name = new NonTerminal("object_name", typeof (ObjectNameNode));

		private readonly NonTerminal semicolon_opt = new NonTerminal("semicolon_opt");
		private readonly NonTerminal sql_expression_list = new NonTerminal("sql_expression_list");
		private readonly NonTerminal sql_expression = new NonTerminal("sql_expression");
		private readonly NonTerminal sql_simple_expression = new NonTerminal("sql_simple_expression");
		private readonly NonTerminal sql_unary_expression = new NonTerminal("sql_unary_expression", typeof (SqlUnaryExpressionNode));
		private readonly NonTerminal sql_binary_expression = new NonTerminal("sql_binary_expression", typeof (SqlBinaryExpressionNode));
		private readonly NonTerminal sql_between_expression = new NonTerminal("sql_between_expression", typeof (SqlBetweenExpressionNode));
		private readonly NonTerminal sql_case_expression = new NonTerminal("sql_case_expression", typeof (SqlCaseExpressionNode));
		private readonly NonTerminal sql_reference_expression = new NonTerminal("sql_reference_expression", typeof (SqlReferenceExpressionNode));
		private readonly NonTerminal term = new NonTerminal("term");
		private readonly NonTerminal not_opt = new NonTerminal("not_op");
		private readonly NonTerminal tuple = new NonTerminal("tuple", typeof (SqlExpressionTupleNode));
		private readonly NonTerminal unary_op = new NonTerminal("unary_op");
		private readonly NonTerminal binary_op = new NonTerminal("binary_op");
		private readonly NonTerminal binary_op_simple = new NonTerminal("binary_op_simple");
		private readonly NonTerminal logical_op = new NonTerminal("logical_op");
		private readonly NonTerminal subquery_op = new NonTerminal("subquery_op");
		private readonly NonTerminal sql_statement = new NonTerminal("sql_statement");
		private readonly NonTerminal sql_command_end_opt = new NonTerminal("sql_command_end_opt");
		private readonly NonTerminal to_define_data = new NonTerminal("to_define_data");
		private readonly NonTerminal to_control_data = new NonTerminal("to_control_data");
		private readonly NonTerminal to_modify_data = new NonTerminal("to_modify_data");
		private readonly NonTerminal create_command = new NonTerminal("create_command");
		private readonly NonTerminal create_schema = new NonTerminal("create_schema");
		private readonly NonTerminal create_table = new NonTerminal("create_table", typeof(CreateTableNode));
		private readonly NonTerminal column_or_constraint_list = new NonTerminal("column_or_constraint_list");
		private readonly NonTerminal column_or_constraint = new NonTerminal("column_or_constraint");
		private readonly NonTerminal table_column = new NonTerminal("table_column", typeof(TableColumnNode));
		private readonly NonTerminal column_name = new NonTerminal("column_name");
		private readonly NonTerminal datatype = new NonTerminal("datatype", typeof (DataTypeNode));
		private readonly NonTerminal number_precision = new NonTerminal("number_precision");
		private readonly NonTerminal character_type = new NonTerminal("character_type");
		private readonly NonTerminal locale_opt = new NonTerminal("locale_opt");
		private readonly NonTerminal integer_type = new NonTerminal("integer_type");
		private readonly NonTerminal decimal_type = new NonTerminal("decimal_type");
		private readonly NonTerminal float_type = new NonTerminal("float_type");
		private readonly NonTerminal date_type = new NonTerminal("date_type");
		private readonly NonTerminal interval_type = new NonTerminal("interval_type");
		private readonly NonTerminal interval_format_opt = new NonTerminal("interval_format_opt");
		private readonly NonTerminal datatype_size = new NonTerminal("datatype_size");
		private readonly NonTerminal long_varchar = new NonTerminal("long_varchar");
		private readonly NonTerminal binary_type = new NonTerminal("binary_type");
		private readonly NonTerminal long_varbinary = new NonTerminal("long_varbinary");
		private readonly NonTerminal user_type = new NonTerminal("user_type");
		private readonly NonTerminal row_type = new NonTerminal("row_type");
		private readonly NonTerminal column_default_opt = new NonTerminal("column_default_opt");
		private readonly NonTerminal column_constraint_list = new NonTerminal("column_constraint_list");
		private readonly NonTerminal column_constraint = new NonTerminal("column_constraint", typeof(ColumnConstraintNode));
		private readonly NonTerminal column_constraint_ref_opt = new NonTerminal("column_constraint_ref_opt");
		private readonly NonTerminal fkey_action_list = new NonTerminal("fkey_action_list");
		private readonly NonTerminal fkey_action = new NonTerminal("fkey_action");
		private readonly NonTerminal fkey_action_type = new NonTerminal("fkey_action_type");
		private readonly NonTerminal table_constraint = new NonTerminal("table_constraint", typeof(TableConstraintNode));
		private readonly NonTerminal constraint_name = new NonTerminal("constraint_name");
		private readonly NonTerminal table_constraint_name_opt = new NonTerminal("table_constraint_name_opt");
		private readonly NonTerminal def_table_constraint = new NonTerminal("def_table_constraint");
		private readonly NonTerminal column_list = new NonTerminal("column_list");
		private readonly NonTerminal function_call_expression = new NonTerminal("function_call_expression", typeof (SqlFunctionCallExpressionNode));
		private readonly NonTerminal function_call_args_opt = new NonTerminal("function_call_args_opt");
		private readonly NonTerminal function_call_args_list = new NonTerminal("function_call_args_list");
		private readonly NonTerminal create_view = new NonTerminal("create_view", typeof(CreateViewNode));
		private readonly NonTerminal or_replace_opt = new NonTerminal("or_replace_opt");
		private readonly NonTerminal if_not_exists_opt = new NonTerminal("if_not_exists_opt");
		private readonly NonTerminal create_index = new NonTerminal("create_index");
		private readonly NonTerminal create_sequence = new NonTerminal("create_sequence");
		private readonly NonTerminal sequence_increment_opt = new NonTerminal("sequence_increment_opt");
		private readonly NonTerminal sequence_start_opt = new NonTerminal("sequence_start_opt");
		private readonly NonTerminal sequence_minvalue_opt = new NonTerminal("sequence_minvalue_opt");
		private readonly NonTerminal sequence_maxvalue_opt = new NonTerminal("sequence_maxvalue_opt");
		private readonly NonTerminal sequence_cache_opt = new NonTerminal("sequence_cache_opt");
		private readonly NonTerminal sequence_cycle_opt = new NonTerminal("sequence_cycle_opt");
		private readonly NonTerminal create_trigger = new NonTerminal("create_trigger", typeof(CreateTriggerNode));
		private readonly NonTerminal create_procedure_trigger = new NonTerminal("create_procedure_trigger");
		private readonly NonTerminal create_callback_trigger = new NonTerminal("create_callback_trigger");
		private readonly NonTerminal before_or_after = new NonTerminal("before_or_after");
		private readonly NonTerminal trigger_events = new NonTerminal("trigger_events");
		private readonly NonTerminal trigger_event = new NonTerminal("trigger_event");
		private readonly NonTerminal trigger_body = new NonTerminal("trigger_body");
		private readonly NonTerminal create_user = new NonTerminal("create_user");
		private readonly NonTerminal identified_rule = new NonTerminal("identified");
		private readonly NonTerminal set_account_lock_opt = new NonTerminal("set_account_lock_opt");
		private readonly NonTerminal set_groups_opt = new NonTerminal("set_groups_opt");
		private readonly NonTerminal select_command = new NonTerminal("select_command", typeof(SelectStatementNode));
		private readonly NonTerminal query = new NonTerminal("query");
		private readonly NonTerminal sql_query_expression = new NonTerminal("sql_query_expression", typeof (SqlQueryExpressionNode));
		private readonly NonTerminal select_into_opt = new NonTerminal("select_into_opt");
		private readonly NonTerminal select_set = new NonTerminal("select_set");
		private readonly NonTerminal select_restrict_opt = new NonTerminal("select_restrict_opt");
		private readonly NonTerminal select_item = new NonTerminal("select_item", typeof (SelectItemNode));
		private readonly NonTerminal select_as_opt = new NonTerminal("selct_as_opt");
		private readonly NonTerminal select_source = new NonTerminal("select_source");
		private readonly NonTerminal as_opt = new NonTerminal("as_opt");
		private readonly NonTerminal select_item_list = new NonTerminal("select_item_list");
		private readonly NonTerminal from_clause_opt = new NonTerminal("from_clause_opt");
		private readonly NonTerminal from_clause = new NonTerminal("from_clause", typeof (FromClauseNode));
		private readonly NonTerminal from_source_list = new NonTerminal("from_source_list");
		private readonly NonTerminal from_source = new NonTerminal("from_source");
		private readonly NonTerminal from_table_source = new NonTerminal("from_table_source", typeof (FromTableSourceNode));
		private readonly NonTerminal from_query_source = new NonTerminal("from_query_source", typeof (FromQuerySourceNode));
		private readonly NonTerminal join_opt = new NonTerminal("join_opt");
		private readonly NonTerminal join = new NonTerminal("join", typeof (JoinNode));
		private readonly NonTerminal join_type = new NonTerminal("join_type");
		private readonly NonTerminal where_clause_opt = new NonTerminal("where_clause_opt", typeof (WhereClauseNode));
		private readonly NonTerminal group_by_opt = new NonTerminal("group_by_opt");
		private readonly NonTerminal having_clause_opt = new NonTerminal("having_clause_opt");
		private readonly NonTerminal order_opt = new NonTerminal("order_opt");
		private readonly NonTerminal sorted_def = new NonTerminal("sorted_def", typeof (OrderByNode));
		private readonly NonTerminal sorted_def_list = new NonTerminal("sorted_def_list");
		private readonly NonTerminal sort_order = new NonTerminal("sort_order");
		private readonly NonTerminal query_composite_opt = new NonTerminal("query_composite_opt");
		private readonly NonTerminal query_composite = new NonTerminal("query_composite", typeof (QueryCompositeNode));
		private readonly NonTerminal any_op = new NonTerminal("any_op");
		private readonly NonTerminal all_op = new NonTerminal("all_op");
		private readonly NonTerminal case_test_expression_opt = new NonTerminal("case_test_expression_opt");
		private readonly NonTerminal case_when_then_list = new NonTerminal("case_when_then_list");
		private readonly NonTerminal case_when_then = new NonTerminal("case_when_then", typeof (CaseSwitchNode));
		private readonly NonTerminal case_else_opt = new NonTerminal("case_else_opt");
		private readonly NonTerminal sql_varef_expression = new NonTerminal("sql_varef_expression", typeof (SqlVariableRefExpressionNode));
		private readonly NonTerminal sql_constant_expression = new NonTerminal("sql_constant_expression", typeof (SqlConstantExpressionNode));

		#region PL/SQL

		private readonly NonTerminal plsql_block = new NonTerminal("plsql_block");
		private readonly NonTerminal label_statement = new NonTerminal("plsql_label");
		private readonly NonTerminal label_statement_opt = new NonTerminal("plsql_label_opt");
		private readonly NonTerminal declare_statement_opt = new NonTerminal("declare_statement_opt");
		private readonly NonTerminal declare_spec = new NonTerminal("declare_spec");
		private readonly NonTerminal declare_spec_list = new NonTerminal("declare_spec_list");
		private readonly NonTerminal declare_variable = new NonTerminal("declare_variable");
		private readonly NonTerminal constant_opt = new NonTerminal("constant_opt");
		private readonly NonTerminal var_not_null_opt = new NonTerminal("var_not_null_opt");
		private readonly NonTerminal var_default_opt = new NonTerminal("var_default_opt");
		private readonly NonTerminal var_default_assign = new NonTerminal("var_default_assign");
		private readonly NonTerminal plsql_code_block = new NonTerminal("plsql_code_block");
		private readonly NonTerminal plsql_statement_list = new NonTerminal("plsql_statement_list");
		private readonly NonTerminal plsql_statement = new NonTerminal("plsql_statement");
		private readonly NonTerminal plsql_sql_statement = new NonTerminal("plsql_sql_statement");
		private readonly NonTerminal exit_statement = new NonTerminal("exit_statement");
		private readonly NonTerminal label_opt = new NonTerminal("label_opt");
		private readonly NonTerminal exit_when_opt = new NonTerminal("exit_when_opt");
		private readonly NonTerminal goto_statement = new NonTerminal("goto_statement");
		private readonly NonTerminal conditional_statement = new NonTerminal("conditional_statement");
		private readonly NonTerminal conditional_elsif_list = new NonTerminal("conditional_elsif_list");
		private readonly NonTerminal conditional_elsif = new NonTerminal("conditional_elsif");
		private readonly NonTerminal conditional_else_opt = new NonTerminal("conditional_else_opt");
		private readonly NonTerminal declare_exception = new NonTerminal("declare_exception");
		private readonly NonTerminal declare_pragma = new NonTerminal("declare_pragma");
		private readonly NonTerminal declare_cursor = new NonTerminal("declare_cursor");
		private readonly NonTerminal cursor_args_opt = new NonTerminal("cursor_args_opt");

		#endregion

		#endregion

		private void Comments() {
			var comment = new CommentTerminal("multiline_comment", "/*", "*/");
			var lineComment = new CommentTerminal("singleline_comment", "--", "\n", "\r\n");
			NonGrammarTerminals.Add(comment);
			NonGrammarTerminals.Add(lineComment);
		}

		private void Literals() {
			string_literal = new StringLiteral("string", "'", StringOptions.AllowsAllEscapes, typeof (StringLiteralNode));
			number_literal = new NumberLiteral("number", NumberOptions.DisableQuickParse | NumberOptions.AllowSign, typeof (NumberLiteralNode));
			positive_literal = new NumberLiteral("positive", NumberOptions.IntOnly, typeof (IntegerLiteralNode));
		}

		private void MakeSimpleId() {
			simple_id = new IdentifierTerminal("simple_id");
			var idStringLiteral = new StringLiteral("simple_id_quoted");
			idStringLiteral.AddStartEnd("\"", StringOptions.NoEscapes);
			idStringLiteral.AstConfig.NodeType = typeof (IdentifierNode);
			idStringLiteral.SetOutputTerminal(this, simple_id);
		}

		private void Operators() {
			RegisterOperators(10, "*", "/", "%");
			RegisterOperators(9, "+", "-");
			RegisterOperators(8, "=", ">", "<", ">=", "<=", "<>", "!=");
			RegisterOperators(8, LIKE, IN);
			RegisterOperators(7, "^", "&", "|");
			RegisterOperators(6, NOT);
			RegisterOperators(5, AND);
			RegisterOperators(4, OR);
		}

		private void SetupRoot() {
			var root = new NonTerminal("root");
			// SQL
			var command_list = new NonTerminal("command_list", typeof(SequenceOfStatementsNode));
			var command = new NonTerminal("command");
			command.Rule = sql_statement + sql_command_end_opt;
			command_list.Rule = MakePlusRule(command_list, command);

			// PL/SQL
			var block = plsql_block + sql_command_end_opt;
			var block_list = new NonTerminal("block_list");
			block_list.Rule = MakePlusRule(block_list, block);

			root.Rule = command_list | block_list;

			Root = root;
		}

		public void SetRootToExpression() {
			Root = sql_expression;
		}

		public void SetRootToDataType() {
			Root = datatype;
		}

		private void Productions() {
			semicolon_opt.Rule = Empty | semicolon;
			not_opt.Rule = Empty | NOT;
			as_opt.Rule = Empty | AS;

			sql_varef_expression.Rule = colon + simple_id;

			or_replace_opt.Rule = Empty | OR + REPLACE;

			object_name.Rule = MakePlusRule(object_name, dot, simple_id);

			sql_command_end_opt.Rule = Empty | Eof | ";";

			sql_statement.Rule = to_define_data | to_modify_data;
			to_define_data.Rule = create_command;
			to_modify_data.Rule = select_command;

			SqlExpression();

			DataType();

			// -- CREATE
			create_command.Rule = create_schema |
			                      create_table |
			                      create_view |
			                      create_index |
								  create_sequence |
								  create_trigger |
								  create_user;

			CreateSchema();
			CreateTable();
			CreateView();
			CreateSequence();
			CreateIndex();
			CreateTrigger();
			CreateUser();

			Query();

			PlSqlBlock();
		}

		private void DataType() {
			// TODO: Refactor this a lot ...
			datatype.Rule = character_type | date_type | integer_type | decimal_type | float_type | binary_type | row_type | user_type;
			character_type.Rule = CHAR + datatype_size + locale_opt |
			                      VARCHAR + datatype_size + locale_opt |
			                      long_varchar + datatype_size + locale_opt;
			locale_opt.Rule = Empty | LOCALE + string_literal;
			date_type.Rule = DATE | TIME | TIMESTAMP;
			integer_type.Rule = INT | INTEGER | BIGINT | SMALLINT | TINYINT;
			decimal_type.Rule = DECIMAL + number_precision | NUMERIC + number_precision | NUMBER + number_precision;
			float_type.Rule = FLOAT | REAL | DOUBLE;
			binary_type.Rule = BINARY + datatype_size | VARBINARY + datatype_size | BLOB | long_varbinary + datatype_size;
			long_varchar.Rule = LONG + VARCHAR;
			long_varbinary.Rule = LONG + VARBINARY;
			row_type.Rule = object_name + "%" + ROWTYPE;
			user_type.Rule = object_name;
			interval_type.Rule = INTERVAL + interval_format_opt;
			interval_format_opt.Rule = YEAR + TO + MONTH | DAY + TO + SECOND;

			datatype_size.Rule = Empty | "(" + positive_literal + ")";

			number_precision.Rule = Empty |
			                        "(" + positive_literal + ")" |
			                        "(" + positive_literal + "," + positive_literal + ")";
		}

		private void SqlExpression() {
			sql_expression_list.Rule = MakePlusRule(sql_expression_list, comma, sql_expression);
			sql_expression.Rule = sql_simple_expression |
			                      sql_between_expression |
			                      sql_case_expression |
			                      sql_query_expression;
			sql_constant_expression.Rule = string_literal | number_literal | TRUE | FALSE | NULL;
			sql_simple_expression.Rule = term | sql_unary_expression | sql_binary_expression;
			term.Rule = sql_reference_expression |
			            sql_varef_expression |
			            sql_constant_expression |
			            function_call_expression |
			            tuple;
			sql_reference_expression.Rule = object_name;
			tuple.Rule = "(" + sql_expression_list + ")";
			sql_unary_expression.Rule = unary_op + term;
			unary_op.Rule = NOT | "+" | "-" | "~";
			sql_binary_expression.Rule = sql_simple_expression + binary_op + sql_simple_expression;
			binary_op_simple.Rule = ToTerm("+") | "-" | "*" | "/" | "%" | ">" | "<" | "=" | "<>";
			binary_op.Rule = binary_op_simple | all_op | any_op | logical_op | subquery_op;
			logical_op.Rule = AND | OR | "&" | "|";
			subquery_op.Rule = IN | NOT + IN;
			any_op.Rule = ANY + binary_op_simple;
			all_op.Rule = ALL + binary_op_simple;
			sql_between_expression.Rule = sql_simple_expression + not_opt + BETWEEN + sql_simple_expression + AND + sql_simple_expression;
			sql_case_expression.Rule = CASE + case_test_expression_opt + case_when_then_list + case_else_opt + END;
			case_test_expression_opt.Rule = Empty | sql_expression;
			case_else_opt.Rule = Empty | ELSE + sql_expression;
			case_when_then_list.Rule = MakePlusRule(case_when_then_list, case_when_then);
			case_when_then.Rule = WHEN + sql_expression + THEN + sql_expression;

			function_call_expression.Rule = object_name + function_call_args_opt;
			function_call_args_opt.Rule = Empty | "(" + function_call_args_list + ")";
			function_call_args_list.Rule = MakeStarRule(function_call_args_list, comma, sql_expression);

			MarkTransient(sql_expression, term, sql_simple_expression, function_call_args_opt);
		}

		private void PlSqlBlock() {
			plsql_block.Rule = label_statement_opt + declare_statement_opt + plsql_code_block;
			label_statement_opt.Rule = Empty | label_statement;
			label_statement.Rule = "<<" + simple_id + ">>";
			declare_statement_opt.Rule = Empty | DECLARE + declare_spec_list;
			declare_spec_list.Rule = MakePlusRule(declare_spec_list, semicolon, declare_spec);
			declare_spec.Rule = declare_variable | declare_exception | declare_pragma;
			declare_variable.Rule = simple_id + constant_opt + datatype + var_not_null_opt + var_default_opt;
			constant_opt.Rule = Empty | CONSTANT;
			var_not_null_opt.Rule = Empty | NOT + NULL;
			var_default_opt.Rule = Empty | var_default_assign + sql_expression;
			var_default_assign.Rule = ":=" | DEFAULT;
			declare_exception.Rule = simple_id + EXCEPTION;
			declare_pragma.Rule = PRAGMA + EXCEPTION_INIT + simple_id + "(" + string_literal + "," + positive_literal + ")";
			declare_cursor.Rule = CURSOR + simple_id + cursor_args_opt + IS + query;

			plsql_code_block.Rule = BEGIN + plsql_statement_list + END;
			plsql_statement_list.Rule = MakePlusRule(plsql_statement_list, plsql_statement);
			plsql_statement.Rule = plsql_sql_statement |
			                       exit_statement |
			                       goto_statement |
			                       conditional_statement;

			plsql_sql_statement.Rule = query;

			exit_statement.Rule = EXIT + label_opt + exit_when_opt;
			label_opt.Rule = Empty | simple_id;
			exit_when_opt.Rule = Empty | WHEN + sql_expression;

			goto_statement.Rule = GOTO + simple_id;

			conditional_statement.Rule = IF + sql_expression + THEN + plsql_statement_list +
			                             conditional_elsif_list +
			                             conditional_else_opt +
			                             END + IF;
			conditional_elsif_list.Rule = MakeStarRule(conditional_elsif_list, conditional_elsif);
			conditional_elsif.Rule = ELSIF + sql_expression + THEN + plsql_statement_list;
			conditional_else_opt.Rule = Empty | ELSE + plsql_statement_list;
		}

		private void CreateTable() {
			create_table.Rule = CREATE + TABLE + if_not_exists_opt + object_name + "(" + column_or_constraint_list + ")";
			if_not_exists_opt.Rule = Empty | IF + NOT + EXISTS;

			column_or_constraint_list.Rule = MakePlusRule(column_or_constraint_list, comma, column_or_constraint);

			column_or_constraint.Rule = table_column | table_constraint;

			table_column.Rule = column_name + datatype + column_constraint_list + column_default_opt;

			column_name.Rule = simple_id;

			column_constraint_list.Rule = MakeStarRule(column_constraint_list, column_constraint);
			column_constraint.Rule = NULL |
			                         NOT + NULL |
			                         UNIQUE |
			                         PRIMARY + KEY |
			                         CHECK + sql_expression |
			                         REFERENCES + object_name + column_constraint_ref_opt + fkey_action_list;
			column_constraint_ref_opt.Rule = Empty | "(" + column_name + ")";
			column_default_opt.Rule = Empty | DEFAULT + sql_expression;
			fkey_action_list.Rule = MakeStarRule(fkey_action_list, fkey_action);
			fkey_action.Rule = ON + DELETE + fkey_action_type | ON + UPDATE + fkey_action_type;
			fkey_action_type.Rule = CASCADE | SET + NULL | SET + DEFAULT | NO + ACTION;

			table_constraint.Rule = table_constraint_name_opt + def_table_constraint;
			table_constraint_name_opt.Rule = Empty | CONSTRAINT + constraint_name;
			constraint_name.Rule = simple_id;
			def_table_constraint.Rule = PRIMARY + KEY + "(" + column_list + ")" |
			                            UNIQUE + "(" + column_list + ")" |
			                            CHECK + "(" + sql_expression + ")" |
			                            FOREIGN + KEY + "(" + column_list + ")" + REFERENCES + object_name + "(" + column_list + ")" +
			                            fkey_action_list;
			column_list.Rule = MakePlusRule(column_list, comma, column_name);
		}

		private void CreateView() {
			create_view.Rule = CREATE + or_replace_opt + VIEW + object_name + AS + query;
		}

		private void CreateUser() {
			create_user.Rule = CREATE + USER + simple_id + identified_rule;
			identified_rule.Rule = IDENTIFIED + BY + PASSWORD + string_literal + set_account_lock_opt + set_groups_opt |
			                       IDENTIFIED + BY + string_literal + set_account_lock_opt + set_groups_opt |
			                       IDENTIFIED + EXTERNALLY;
			set_account_lock_opt.Rule = SET + ACCOUNT + LOCK |
			                            SET + ACCOUNT + UNLOCK |
			                            Empty;
			set_groups_opt.Rule = SET + GROUPS + string_literal | Empty;
		}

		private void CreateIndex() {
			create_index.Rule = CREATE + INDEX + object_name + ON + object_name + "(" + column_list + ")";
		}

		private void CreateSequence() {
			create_sequence.Rule = CREATE + SEQUENCE + object_name + 
				sequence_increment_opt + 
				sequence_start_opt +
				sequence_minvalue_opt + 
				sequence_maxvalue_opt +
				sequence_cache_opt +
				sequence_cycle_opt;
			sequence_increment_opt.Rule = INCREMENT + BY + sql_expression | Empty;
			sequence_start_opt.Rule = START + WITH + sql_expression | Empty;
			sequence_minvalue_opt.Rule = MINVALUE + sql_expression | Empty;
			sequence_maxvalue_opt.Rule = MAXVALUE + sql_expression | Empty;
			sequence_cycle_opt.Rule = CYCLE | Empty;
			sequence_cache_opt.Rule = CACHE + sql_expression | Empty;
		}

		private void CreateSchema() {
			create_schema.Rule = CREATE + SCHEMA + simple_id;
		}

		private void CreateTrigger() {
			create_trigger.Rule = create_procedure_trigger | create_callback_trigger;
			create_callback_trigger.Rule = CREATE + or_replace_opt + CALLBACK + TRIGGER +
			                               before_or_after + ON + object_name;
			create_procedure_trigger.Rule = CREATE + or_replace_opt + TRIGGER + object_name +
			                                before_or_after + ON + object_name +
											FOR + EACH + ROW;
			before_or_after.Rule = BEFORE | AFTER;
			trigger_events.Rule = MakePlusRule(trigger_events, OR, trigger_event);
			trigger_event.Rule = INSERT | UPDATE | DELETE;
			trigger_body.Rule = EXECUTE + PROCEDURE + object_name + "(" + function_call_args_list + ")" |
			                    plsql_block;
		}

		private void Query() {
			query.Rule = sql_query_expression;
			select_command.Rule = query;

			sql_query_expression.Rule = SELECT +
			                            select_restrict_opt +
			                            select_into_opt +
			                            select_set +
			                            from_clause_opt +
			                            where_clause_opt +
			                            group_by_opt +
			                            query_composite_opt;

			select_restrict_opt.Rule = Empty | ALL | DISTINCT;
			select_into_opt.Rule = Empty | INTO + object_name;
			select_set.Rule = select_item_list | "*";
			select_item_list.Rule = MakePlusRule(select_item_list, comma, select_item);
			select_item.Rule = select_source + select_as_opt;
			select_as_opt.Rule = as_opt + simple_id | Empty;
			select_source.Rule = sql_expression | object_name;
			from_clause_opt.Rule = Empty | from_clause;
			from_clause.Rule = FROM + from_source_list;
			from_source_list.Rule = MakePlusRule(from_source_list, from_source);
			from_source.Rule = from_table_source | from_query_source;
			from_table_source.Rule = object_name + select_as_opt + join_opt;
			from_query_source.Rule = "(" + query + ")" + select_as_opt + join_opt;
			join_opt.Rule = Empty | join;
			join.Rule = "," + from_table_source |
			            join_type + JOIN + from_table_source + ON + sql_expression;
			join_type.Rule = INNER | OUTER | LEFT | LEFT + OUTER | RIGHT | RIGHT + OUTER;
			where_clause_opt.Rule = Empty | WHERE + sql_expression_list;
			group_by_opt.Rule = Empty | GROUP + BY + sql_expression_list + having_clause_opt;
			having_clause_opt.Rule = Empty | HAVING + sql_expression;
			query_composite_opt.Rule = Empty | query_composite;
			query_composite.Rule = UNION + all_op + sql_query_expression |
			                       INTERSECT + all_op + sql_query_expression |
			                       EXCEPT + all_op + sql_query_expression;
			order_opt.Rule = Empty | ORDER + BY + sorted_def_list;
			sorted_def.Rule = sql_expression + sort_order;
			sort_order.Rule = ASC | DESC;
			sorted_def_list.Rule = MakePlusRule(sorted_def_list, comma, sorted_def);

			MarkTransient(from_source, select_as_opt);
		}
	}
}