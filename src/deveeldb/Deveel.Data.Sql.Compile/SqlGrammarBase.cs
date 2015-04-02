using System;

using Irony.Parsing;

namespace Deveel.Data.Sql.Compile {
	abstract class SqlGrammarBase : Grammar {
		// cached most used non-terminal
		private NonTerminal sqlExpression;
		private NonTerminal objectname;
		private NonTerminal datatype;

		protected SqlGrammarBase()
			: base(false) {
			SetupGrammar();
		}

		public abstract string Dialect { get; }

		protected StringLiteral StringLiteral { get; private set; }

		protected NumberLiteral NumberLiteral { get; private set; }

		protected NumberLiteral PositiveLiteral { get; private set; }

		protected IdentifierTerminal Identifier { get; private set; }

		protected KeyTerm Comma { get; private set; }

		protected KeyTerm Dot { get; private set; }

		protected KeyTerm Colon { get; private set; }

		protected KeyTerm As { get; private set; }

		protected abstract NonTerminal MakeRoot();

		private void Comments() {
			var comment = new CommentTerminal("multiline_comment", "/*", "*/");
			var lineComment = new CommentTerminal("singleline_comment", "--", "\n", "\r\n");
			NonGrammarTerminals.Add(comment);
			NonGrammarTerminals.Add(lineComment);
		}

		private void Literals() {
			StringLiteral = new StringLiteral("string", "'", StringOptions.AllowsAllEscapes, typeof(StringLiteralNode));
			NumberLiteral = new NumberLiteral("number", NumberOptions.DisableQuickParse | NumberOptions.AllowSign, typeof(NumberLiteralNode));
			PositiveLiteral = new NumberLiteral("positive", NumberOptions.IntOnly, typeof(IntegerLiteralNode));
		}

		private void MakeSimpleId() {
			Identifier = new IdentifierTerminal("simple_id");
			var idStringLiteral = new StringLiteral("simple_id_quoted");
			idStringLiteral.AddStartEnd("\"", StringOptions.NoEscapes);
			idStringLiteral.AstConfig.NodeType = typeof(IdentifierNode);
			idStringLiteral.SetOutputTerminal(this, Identifier);
		}

		private void Operators() {
			RegisterOperators(10, "*", "/", "%");
			RegisterOperators(9, "+", "-");
			RegisterOperators(8, "=", ">", "<", ">=", "<=", "<>", "!=");
			RegisterOperators(8, Key("LIKE"), Key("IN"));
			RegisterOperators(7, "^", "&", "|");
			RegisterOperators(6, Key("NOT"));
			RegisterOperators(5, Key("AND"));
			RegisterOperators(4, Key("OR"));
		}

		private void SetupGrammar() {
			Comma = ToTerm(",");
			Dot = ToTerm(".");
			Colon = ToTerm(":");
			As = ToTerm("AS");

			MakeSimpleId();

			Comments();
			Keywords();
			ReservedWords();
			Literals();

			Operators();

			MarkPunctuation(",", "(", ")", "AS");

			Root = MakeRoot();
		}

		protected KeyTerm Key(string term) {
			KeyTerm keyTerm;
			if (!KeyTerms.TryGetValue(term, out keyTerm))
				KeyTerms[term] = keyTerm = ToTerm(term);

			return keyTerm;
		}

		protected virtual void Keywords() {
			
		}

		protected virtual void ReservedWords() {
			
		}

		protected NonTerminal ObjectName() {
			if (objectname != null)
				return objectname;

			objectname = new NonTerminal("object_name", typeof(ObjectNameNode));
			objectname.Rule = MakePlusRule(objectname, Dot, Identifier);
			return objectname;
		}

		protected NonTerminal DataType() {
			if (datatype != null)
				return datatype;

			datatype = new NonTerminal("datatype", typeof (DataTypeNode));
			var numberPrecision = new NonTerminal("number_precision");
			var characterType = new NonTerminal("character_type");
			var localeOpt = new NonTerminal("locale_opt");
			var integerType = new NonTerminal("integer_type");
			var decimalType = new NonTerminal("decimal_type");
			var floatType = new NonTerminal("float_type");
			var dateType = new NonTerminal("date_type");
			var intervalType = new NonTerminal("interval_type");
			var intervalFormatOpt = new NonTerminal("interval_format_opt");
			var datatypeSize = new NonTerminal("datatype_size");
			var longVarchar = new NonTerminal("long_varchar");
			var binaryType = new NonTerminal("binary_type");
			var longVarbinary = new NonTerminal("long_varbinary");
			var userType = new NonTerminal("user_type");
			var rowType = new NonTerminal("row_type");
			var geometryType = new NonTerminal("geometry_type");
			var xmlType = new NonTerminal("xml_type");

			datatype.Rule = characterType |
			                dateType |
			                integerType |
			                decimalType |
			                floatType |
			                binaryType |
			                rowType |
			                userType |
			                geometryType |
			                xmlType;

			characterType.Rule = Key("CHAR") + datatypeSize + localeOpt |
			                     Key("VARCHAR") + datatypeSize + localeOpt |
			                     longVarchar + datatypeSize + localeOpt;
			localeOpt.Rule = Empty | Key("LOCALE") + StringLiteral;
			dateType.Rule = Key("DATE") | Key("TIME") | Key("TIMESTAMP");
			integerType.Rule = Key("INT") |
			                   Key("INTEGER") |
			                   Key("BIGINT") |
			                   Key("SMALLINT") |
			                   Key("TINYINT");
			decimalType.Rule = Key("DECIMAL") + numberPrecision |
			                   Key("NUMERIC") + numberPrecision |
			                   Key("NUMBER") + numberPrecision;
			floatType.Rule = Key("FLOAT") |
			                 Key("REAL") |
			                 Key("DOUBLE");
			binaryType.Rule = Key("BINARY") + datatypeSize |
			                  Key("VARBINARY") + datatypeSize |
			                  Key("BLOB") |
			                  longVarbinary + datatypeSize;
			longVarchar.Rule = Key("LONG") + Key("VARCHAR");
			longVarbinary.Rule = Key("LONG") + Key("VARBINARY");
			rowType.Rule = ObjectName() + "%" + Key("ROWTYPE");
			userType.Rule = ObjectName();
			intervalType.Rule = Key("INTERVAL") + intervalFormatOpt;
			intervalFormatOpt.Rule = Key("YEAR") + Key("TO") + Key("MONTH") |
			                         Key("DAY") + Key("TO") + Key("SECOND");
			geometryType.Rule = Key("GEOMETRY") + datatypeSize;
			xmlType.Rule = Key("XML");

			datatypeSize.Rule = Empty | "(" + PositiveLiteral + ")";

			numberPrecision.Rule = Empty |
			                       "(" + PositiveLiteral + ")" |
			                       "(" + PositiveLiteral + "," + PositiveLiteral + ")";

			return datatype;
		}

		protected NonTerminal SqlExpressionList() {
			var list = new NonTerminal("sql_expression_list");
			list.Rule = MakePlusRule(list, Comma, SqlExpression());
			return list;
		}

		protected NonTerminal SqlQueryExpression() {
			var selectIntoOpt = new NonTerminal("select_into_opt");
			var selectSet = new NonTerminal("select_set");
			var selectRestrictOpt = new NonTerminal("select_restrict_opt");
			var selectItem = new NonTerminal("select_item", typeof(SelectItemNode));
			var selectAsOpt = new NonTerminal("select_as_opt");
			var selectSource = new NonTerminal("select_source");
			var selectItemList = new NonTerminal("select_item_list");
			var fromClauseOpt = new NonTerminal("from_clause_opt");
			var fromClause = new NonTerminal("from_clause", typeof(FromClauseNode));
			var fromSource = new NonTerminal("from_source");
			var fromTableSource = new NonTerminal("from_table_source", typeof(FromTableSourceNode));
			var fromQuerySource = new NonTerminal("from_query_source", typeof(FromQuerySourceNode));
			var joinOpt = new NonTerminal("join_opt");
			var joinType = new NonTerminal("join_type");
			var join = new NonTerminal("join", typeof (JoinNode));
			var onOpt = new NonTerminal("on_opt");
			var whereClauseOpt = new NonTerminal("where_clause_opt", typeof(WhereClauseNode));
			var groupByOpt = new NonTerminal("group_by_opt");
			var havingClauseOpt = new NonTerminal("having_clause_opt");
			var orderOpt = new NonTerminal("order_opt");
			var sortedDef = new NonTerminal("sorted_def", typeof(OrderByNode));
			var sortedDefList = new NonTerminal("sorted_def_list");
			var sortOrder = new NonTerminal("sort_order");
			var queryCompositeOpt = new NonTerminal("query_composite_opt");
			var queryComposite = new NonTerminal("query_composite", typeof(QueryCompositeNode));
			var expression = new NonTerminal("sql_query_expression", typeof(SqlQueryExpressionNode));
			var allOpt = new NonTerminal("all_opt");
			var asOpt = new NonTerminal("as_opt");

			expression.Rule = Key("SELECT") + selectRestrictOpt +
							  selectIntoOpt +
							  selectSet +
							  fromClauseOpt +
							  whereClauseOpt +
							  groupByOpt +
							  queryCompositeOpt;

			selectRestrictOpt.Rule = Empty | Key("ALL") | Key("DISTINCT");
			selectIntoOpt.Rule = Empty | Key("INTO") + ObjectName();
			selectSet.Rule = selectItemList | "*";
			selectItemList.Rule = MakePlusRule(selectItemList, Comma, selectItem);
			selectItem.Rule = selectSource + selectAsOpt;
			selectAsOpt.Rule = Empty |
			                   As + Identifier |
			                   Identifier;
			selectSource.Rule = SqlExpression() | ObjectName();
			fromClauseOpt.Rule = Empty | fromClause;
			fromClause.Rule = Key("FROM") + fromSource + joinOpt;
			fromSource.Rule = fromTableSource |
			                  fromQuerySource;
			fromTableSource.Rule = ObjectName() + selectAsOpt;
			fromQuerySource.Rule = "(" + expression + ")" + selectAsOpt;

			joinOpt.Rule = Empty | join;
			join.Rule = joinType + fromSource + onOpt;
			onOpt.Rule = Empty | Key("ON") + SqlExpression() + joinOpt;
			joinType.Rule = Key("INNER") + Key("JOIN")|
							Key("OUTER") + Key("JOIN") |
							Key("LEFT") + Key("JOIN") |
							Key("LEFT") + Key("OUTER") + Key("JOIN") |
							Key("RIGHT") + Key("JOIN") |
							Key("RIGHT") + Key("OUTER") + Key("JOIN") |
							Comma;
			whereClauseOpt.Rule = Empty | Key("WHERE") + SqlExpressionList();
			groupByOpt.Rule = Empty | Key("GROUP") + Key("BY") + SqlExpressionList() + havingClauseOpt;
			havingClauseOpt.Rule = Empty | Key("HAVING") + SqlExpression();
			queryCompositeOpt.Rule = Empty | queryComposite;
			queryComposite.Rule = Key("UNION") + allOpt + expression |
								   Key("INTERSECT") + allOpt + expression |
								   Key("EXCEPT") + allOpt + expression;
			orderOpt.Rule = Empty | Key("ORDER") + Key("BY") + sortedDefList;
			sortedDef.Rule = SqlExpression() + sortOrder;
			sortOrder.Rule = Key("ASC") | Key("DESC");
			allOpt.Rule = Empty | Key("ALL");
			asOpt.Rule = Empty | As;
			sortedDefList.Rule = MakePlusRule(sortedDefList, Comma, sortedDef);

			MarkTransient(selectSource);

			return expression;
		}

		protected NonTerminal SqlExpression() {
			if (sqlExpression != null)
				return sqlExpression;

			sqlExpression = new NonTerminal("sql_expression");

			var sqlUnaryExpression = new NonTerminal("sql_unary_expression", typeof(SqlUnaryExpressionNode));
			var sqlBinaryExpression = new NonTerminal("sql_binary_expression", typeof(SqlBinaryExpressionNode));
			var sqlBetweenExpression = new NonTerminal("sql_between_expression", typeof(SqlBetweenExpressionNode));
			var sqlCaseExpression = new NonTerminal("sql_case_expression", typeof(SqlCaseExpressionNode));
			var sqlReferenceExpression = new NonTerminal("sql_reference_expression", typeof(SqlReferenceExpressionNode));
			var term = new NonTerminal("term");
			var sqlSimpleExpression = new NonTerminal("sql_simple_expression");
			var unaryOp = new NonTerminal("unary_op");
			var binaryOp = new NonTerminal("binary_op");
			var binaryOpSimple = new NonTerminal("binary_op_simple");
			var logicalOp = new NonTerminal("logical_op");
			var subqueryOp = new NonTerminal("subquery_op");
			var caseTestExpressionOpt = new NonTerminal("case_test_expression_opt");
			var caseWhenThenList = new NonTerminal("case_when_then_list");
			var caseWhenThen = new NonTerminal("case_when_then", typeof(CaseSwitchNode));
			var caseElseOpt = new NonTerminal("case_else_opt");
			var sqlVarefExpression = new NonTerminal("sql_varef_expression", typeof(SqlVariableRefExpressionNode));
			var sqlConstantExpression = new NonTerminal("sql_constant_expression", typeof(SqlConstantExpressionNode));
			var functionCallExpression = new NonTerminal("function_call_expression", typeof(SqlFunctionCallExpressionNode));
			var functionCallArgsOpt = new NonTerminal("function_call_args_opt");
			var functionCallArgsList = new NonTerminal("function_call_args_list");
			var notOpt = new NonTerminal("not_op");
			var tuple = new NonTerminal("tuple", typeof(SqlExpressionTupleNode));
			var anyOp = new NonTerminal("any_op");
			var allOp = new NonTerminal("all_op");

			sqlExpression.Rule = sqlSimpleExpression |
								  sqlBetweenExpression |
								  sqlCaseExpression |
								  SqlQueryExpression();
			sqlConstantExpression.Rule = StringLiteral | NumberLiteral | Key("TRUE") | Key("FALSE") | Key("NULL");
			sqlSimpleExpression.Rule = term | sqlUnaryExpression | sqlBinaryExpression;
			term.Rule = sqlReferenceExpression |
						sqlVarefExpression |
						sqlConstantExpression |
						functionCallExpression |
						tuple;
			sqlReferenceExpression.Rule = ObjectName();
			tuple.Rule = "(" + SqlExpressionList() + ")";
			sqlUnaryExpression.Rule = unaryOp + term;
			unaryOp.Rule = Key("NOT") | "+" | "-" | "~";
			sqlBinaryExpression.Rule = sqlSimpleExpression + binaryOp + sqlSimpleExpression;
			binaryOpSimple.Rule = ToTerm("+") | "-" | "*" | "/" | "%" | ">" | "<" | "=" | "<>";
			binaryOp.Rule = binaryOpSimple | allOp | anyOp | logicalOp | subqueryOp;
			logicalOp.Rule = Key("AND") | Key("OR") | "&" | "|";
			subqueryOp.Rule = Key("IN") | Key("NOT") + Key("IN");
			anyOp.Rule = Key("ANY") + binaryOpSimple;
			allOp.Rule = Key("ALL") + binaryOpSimple;
			sqlBetweenExpression.Rule = sqlSimpleExpression + notOpt + Key("BETWEEN") + sqlSimpleExpression + Key("AND") +
										sqlSimpleExpression;
			sqlCaseExpression.Rule = Key("CASE") + caseTestExpressionOpt + caseWhenThenList + caseElseOpt + Key("END");
			caseTestExpressionOpt.Rule = Empty | sqlExpression;
			caseElseOpt.Rule = Empty | Key("ELSE") + sqlExpression;
			caseWhenThenList.Rule = MakePlusRule(caseWhenThenList, caseWhenThen);
			caseWhenThen.Rule = Key("WHEN") + sqlExpression + Key("THEN") + sqlExpression;

			functionCallExpression.Rule = ObjectName() + functionCallArgsOpt;
			functionCallArgsOpt.Rule = Empty | "(" + functionCallArgsList + ")";
			functionCallArgsList.Rule = MakeStarRule(functionCallArgsList, Comma, sqlExpression);

			sqlVarefExpression.Rule = Colon + Identifier;

			notOpt.Rule = Empty | Key("NOT");

			MarkTransient(sqlExpression, term, sqlSimpleExpression, functionCallArgsOpt);

			return sqlExpression;
		}
	}
}
