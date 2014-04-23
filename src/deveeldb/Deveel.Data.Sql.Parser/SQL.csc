options {
  STATIC = false;
  UNICODE_INPUT = true;
  OPTIMIZE_TOKEN_MANAGER = true;
//  IGNORE_CASE = true;
  DEBUG_PARSER = false;
  VISIBILITY_INTERNAL = true;
  COMMON_TOKEN_ACTION = true;
}

PARSER_BEGIN(SQL)

namespace Deveel.Data.Sql;

using System;
using System.Collections;
using System.Collections.Generic;

using DbSystem;
using Routines;
using Sql;
using Text;
using Types;

internal class SQL {

	// State variables for the parser,
	
	/// <summary>
	/// Set to true if the SQL identifiers are converted to upper case.
	/// </summary>
	private bool case_insensitive_identifiers = false;
	
	/// <summary>
	/// The parameter id.
	/// </summary>
	private int parameter_id = 0;
	private ParameterStyle parameterStyle = ParameterStyle.Marker;
	
	/// <summary>
	/// Resets the parameter id.
	/// </summary>
	/// <remarks>
	/// This MUST be called before a parser is used to parse a statement.
	/// </remarks>
	public void Reset() {
		parameter_id = 0;
		token_source.tokenHistory.Clear();
	}
		
	/// <summary>
	/// Creates and returns a parameter substitution.
	/// </summary>
	/// <remarks>
	/// This is called when the parser comes across a <c>?</c> style object. This 
	/// object is used to mark an expression with a place mark that can be substituted 
	/// for a value later.
	/// </remarks>
	public ParameterSubstitution CreateSubstitution(String image) {
		ParameterSubstitution ps;
		if (image == null || image.Length == 0 || image.Equals("?")) {
			ps = new ParameterSubstitution(parameter_id);
			++parameter_id;
		} else {
			ps = new ParameterSubstitution(image);
		}
		return ps;
	}

	private VariableRef CreateVariableRef(string image) {
		return new VariableRef(image.Substring(1, image.Length - 1));
	}
  
	/// <summary>
	/// If the parser has been defined as case insensitive then this
	/// returns the uppercase version of the given string.
	/// </summary>
	/// <remarks>
	/// <b>Note</b>: This actually doesn't do anything because the case is now 
	/// resolved outside the parser.
	/// </remarks>
	public String CaseCheck(String identif) {
//		if (case_insensitive_identifiers)
//			return identif.ToUpper();
		return identif;
	}
  
  /**
   * Helper for expression parsing.
   * Called when an end parenthese has been found.
   */
	public void expEndParen(Expression exp, Stack stack) {
		Operator op = (Operator) stack.Pop();
		while (!op.StringRepresentation.Equals("(")) {
			addOperatorToExpression(exp, op);
			op = (Operator) stack.Pop();
		}
	}
  
  /**
   * Helper for expression parsing.
   * Called when an operator has been read in.  This needs to check precedence
   * and add the operator to the expression as appropriate.
   */
  public void expOperator(Expression exp, Stack stack, Operator op) {
    int precedence = op.Precedence;
    flushOperatorStack(exp, stack, precedence);
    stack.Push(op);
  }

  /**
   * Flush the operator stack until the stack is either empty or the top
   * element is either a "(" or of a precedence lower than the given
   * precedence.
   */
  public void flushOperatorStack(Expression exp, Stack stack, int precedence) {
    if (stack.Count > 0) {
      Operator top_op = (Operator) stack.Pop();
      while (!top_op.StringRepresentation.Equals("(") && top_op.Precedence >= precedence) {
        addOperatorToExpression(exp, top_op);
        if (stack.Count == 0) {
          return;
        }
        top_op = (Operator) stack.Pop();
      }
      stack.Push(top_op);
    }
  }
  
  /**
   * Helper for expression parsing.
   * Called when an entire expression has been read in.  We need to empty
   * the stack.
   */
  public void expEnd(Expression exp, Stack stack) {
    while (stack.Count > 0) {
      Operator op = (Operator) stack.Pop();
      addOperatorToExpression(exp, op);
    }
  }

  /**
   * Helper for expression parsing.
   * Adds an operator to the given expression.
   */
  public void addOperatorToExpression(Expression exp, Operator op) {
    if (op.StringRepresentation.Equals("not")) {
      exp.AddElement(null);
    }
    exp.AddOperator(op);
  }
}

PARSER_END(SQL)


SKIP : {

  " "
| "\t"
| "\n"
| "\r"
|  <"//" (~["\n","\r"])* ("\n" | "\r" | "\r\n")>
|  <"--" (~["\n","\r"])* ("\n" | "\r" | "\r\n")>
//|  <"/*" (~["*"])* "*" ("*" | ~["*","/"] (~["*"])* "*")* "/">

}

TOKEN: {

  <STAR:       "*" >

| <ASSIGNMENT: "=" >
| <EQUALS:     "==" >
| <GR:         ">" >
| <LE:         "<" >
| <GREQ:       ">=" >
| <LEEQ:       "<=" >
| <NOTEQ:      "!=" | "<>" >
| <DIVIDE:     "/" >
| <ADD:        "+" >
| <SUBTRACT:   "-" >
| <CONCAT:     "||" >
| <MODULUS:    "%" >
}

TOKEN: {
  <SEMICOLUMN: ";" >
}

TOKEN [IGNORE_CASE] : {
  
    <BOOLEAN_LITERAL: "true" | "false" >
  | <NULL_LITERAL:    "null" >
      // NOTE: Handling regex literals is a horrible horrible hack.  The <REGEX_LITERAL> 
      //   token starts with 'regex /' and the regex string follows.
      //   The reason for this hack is because / clashes with <DIVIDE>
  | <REGEX_LITERAL:   "regex /" (~["/","\n","\r"] | "\\/" )* "/" ( "i" | "s" | "m" )* >
  
}

TOKEN [IGNORE_CASE] : { /* KEYWORDS */

  <DROP:        "drop">
| <SHOW:        "show">
| <ALTER:       "alter">
| <SELECT:      "select">
| <UPDATE:      "update">
| <CREATE:      "create">
| <DELETE:      "delete">
| <INSERT:      "insert">
| <COMMIT:      "commit">
| <COMPACT:     "compact">
| <EXPLAIN:     "explain">
| <ROLLBACK:    "rollback">
| <OPTIMIZE:    "optimize">
| <DESCRIBE:    "describe">
| <SHUTDOWN:    "shutdown">

| <IS:          "is">
| <AS:          "as">
| <ON:          "on">
| <IF:          "if">
| <TO:          "to">
| <NO:          "no">
| <ALL:         "all">
| <ANY:         "any">
| <END:         "end">
| <SET:         "set">
| <USE:         "use">
| <ASC:         "asc">
| <OLD:         "old">
| <NEW:         "new">
| <SQLADD:      "add">
| <FOR:         "for">
| <ROW:         "row">
| <CASE:        "case">
| <ELSE:        "else">
| <EACH:        "each">
| <CALL:        "call">
| <BOTH:        "both">
| <SOME:        "some">
| <FROM:        "from">
| <LEFT:        "left">
| <DESC:        "desc">
| <INTO:        "into">
| <JOIN:        "join">
| <TRIM:        "trim">
| <VIEW:        "view">
| <LOCK:        "lock">
| <WITH:        "with">
| <TYPE:        "type">
| <USER:        "user">
| <CAST:        "cast">
| <LONG:        "long">
| <NAME:        "name">
| <OPEN:        "open">
| <THEN:        "then">
| <WHEN:        "when">
| <AFTER:       "after">
| <START:       "start">
| <COUNT:       "count">
| <WHERE:       "where">
| <CYCLE:       "cycle">
| <CACHE:       "cache">
| <RIGHT:       "right">
| <TABLE:       "table">
| <LIMIT:       "limit">
| <INNER:       "inner">
| <INDEX:       "index">
| <CLOSE:       "close">
| <CROSS:       "cross">
| <OUTER:       "outer">
| <CHECK:       "check">
| <FINAL:       "final">
| <UNDER:       "under">
| <USING:       "using">
| <UNION:       "union">
| <GRANT:       "grant">
| <USAGE:       "usage">
| <FETCH:       "fetch">
| <SQLRETURN:   "return">
| <BEFORE:      "before">
| <UNLOCK:      "unlock">
| <CURSOR:      "cursor">
| <SCROLL:      "scroll">
| <ACTION:      "action">
| <GROUPS:      "groups">
| <REVOKE:      "revoke">
| <OPTION:      "option">
| <CSHARP:      "csharp">
| <PUBLIC:      "public">
| <EXCEPT:      "except">
| <IGNORE:      "ignore">
| <SCHEMA:      "schema">
| <EXISTS:      "exists">
| <SOUNDS:      "sounds">
| <VALUES:      "values">
| <HAVING:      "having">
| <UNIQUE:      "unique">
| <SQLCOLUMN:   "column">
| <RETURNS:     "returns">
| <ACCOUNT:     "account">
| <DECLARE:     "declare">
| <LEADING:     "leading">
| <NATURAL:     "natural">
| <BETWEEN:     "between">
| <TRIGGER:     "trigger">
| <SQLDEFAULT:  "default">
| <VARYING:     "varying">
| <EXECUTE:     "execute">
| <CALLBACK:    "callback">
| <MINVALUE:    "minvalue">
| <MAXVALUE:    "maxvalue">
| <FUNCTION:    "function">
| <CONSTANT:    "constant">
| <READONLY:    "readonly">
| <SEQUENCE:    "sequence">
| <RESTRICT:    "restrict">
| <PASSWORD:    "password">
| <TRAILING:    "trailing">
//| <NOTNULL:     "not null">
| <GROUPBY:     "group by">
| <EXTRACT:     "extract">
| <ORDERBY:     "order by">
| <DEFERRED:    "deferred">
| <EXTERNAL:    "external">
| <IDENTITY:    "identity">
| <DISTINCT:    "distinct">
| <LANGUAGE:    "language">
| <INCREMENT:   "increment">
| <PROCEDURE:   "procedure">
| <CHARACTER:   "character">
| <CURRENTOF:   "current of">
| <IMMEDIATE:   "immediate">
| <INITIALLY:   "initially">
| <TEMPORARY:   "temporary">
| <INTERSECT:   "intersect">
| <PRIVILEGES:  "privileges">
| <CONSTRAINT:  "constraint">
| <DEFERRABLE:  "deferrable">
| <REFERENCES:  "references">
| <INSENSITIVE: "insensitive">

| <PRIMARY:     "primary">
| <FOREIGN:     "foreign">
| <KEY:         "key">

| <INDEX_NONE:  "index_none">
| <INDEX_BLIST: "index_blist">

| <GROUPMAX:    "group max">

| <COLLATE:     "collate">

// Collate strengths,

| <PRIMARY_STRENGTH:   "primary_strength">
| <SECONDARY_STRENGTH: "secondary_strength">
| <TERTIARY_STRENGTH:  "tertiary_strength">
| <IDENTICAL_STRENGTH: "identical_strength">

// Collate decomposition levels,

| <NO_DECOMPOSITION:        "no_decomposition">
| <CANONICAL_DECOMPOSITION: "canonical_decomposition">
| <FULL_DECOMPOSITION:      "full_decomposition">

// Data types,

| <BIT:         "bit">
| <DAY:         "day">
| <INT:         "int">
| <REAL:        "real">
| <CLOB:        "clob">
| <BLOB:        "blob">
| <CHAR:        "char">
| <TEXT:        "text">
| <DATE:        "date">
| <TIME:        "time">
| <YEAR:        "year">
| <HOUR:        "hour">
| <FLOAT:       "float">
| <MONTH:       "month">
| <SECOND:      "second">
| <MINUTE:      "minute">
| <BIGINT:      "bigint">
| <DOUBLE:      "double">
| <STRING:      "string">
| <BINARY:      "binary">
| <OBJECT:      "object">
| <NUMERIC:     "numeric">
| <DECIMAL:     "decimal">
| <BOOLEAN:     "bool">
| <TINYINT:     "tinyint">
| <INTEGER:     "integer">
| <VARCHAR:     "varchar">
| <SMALLINT:    "smallint">
| <INTERVAL:    "interval">
| <VARBINARY:   "varbinary">
| <TIMESTAMP:   "timestamp">
| <LONGVARCHAR: "longvarchar">
| <LONGVARBINARY: "longvarbinary">

| <TRANSACTIONISOLATIONLEVEL: "transaction isolation level">
| <AUTOCOMMIT:                "auto commit">
| <READCOMMITTED:             "read committed">
| <READUNCOMMITTED:           "read uncommitted">
| <REPEATABLEREAD:            "repeatable read">
| <SERIALIZABLE:              "serializable">

// Fetch orientations
| <FIRST:        "first">
| <LAST:         "last">
| <NEXT:         "next">
| <PRIOR:        "prior">
| <ABSOLUTE:     "absolute">
| <RELATIVE:     "relative">

| <CASCADE:                   "cascade">

// Current date/time/timestamp literals

| <CURRENT_TIME:      "current_time">
| <CURRENT_DATE:      "current_date">
| <CURRENT_TIMESTAMP: "current_timestamp">

// Current timezone

| <DBTIMEZONE:        "dbtimezone">



//| <DATA_TYPE: "bool" | "bit" | "tinyint" | "smallint" | "integer" | "bigint" | "float" | "real" |
//              "double" | "numeric" | "decimal" | "char" | "varchar" | "longvarchar" | "string" |
//              "date" | "time" | "timestamp" | "binary" | "varbinary" | "longvarbinary" >

// NOTE: OPERATOR doesn't match '*' or '=' because we use * and = to mean different things
//  <OPERATOR: ( "==" | ">" | "<" | ">=" | "<=" | "!=" | "<>" | "/" | "+" | "-" |
//               "like" | "not like" | "regex" | "and" | "or" ) >

| <LIKE:       "like" >
//| <NOTLIKE:    "not like" >
| <REGEX:      "regex" >
| <AND:        "and" >
| <OR:         "or" >
| <IN:         "in" >
//| <NOTIN:      "not in" >

| <NOT:        "not">

}

TOKEN : {

    <NUMBER_LITERAL:
       ( ( (["0"-"9"])+ ( "." (["0"-"9"])+ )? )
    | ( "." (["0"-"9"])+ ) )
               ( "E" (["-","+"])? (["0"-"9"])+ )? 
//       (["-","+"])? ( ( (["0"-"9"])+ ( "." (["0"-"9"])+ )? )
//                 |( "." (["0"-"9"])+ ) )
//                            ( "E" (["-","+"])? (["0"-"9"])+ )? 
    
//        ("-")? (["0"-"9"])+ "." (["0"-"9"])*
//      | ("-")? "." (["0"-"9"])+
//      | ("-")? (["0"-"9"])+
//      | 
    >
  | <STRING_LITERAL:   "'" ( "''" | "\\" ["a"-"z", "\\", "%", "_", "'"] | ~["'","\\"] )* "'" >
//  | <STRING_LITERAL:   "'" ( "''" | ~["'","\n","\r","\\"] )* "'" >
//  | <QUOTED_VARIABLE: "\"" ( ["a"-"z", "A"-"Z", "_", "0"-"9", "."] )* "\"" >
  | <QUOTED_VARIABLE:   "\"" ( ~["\""] )* "\"" >
  
}

TOKEN : {  /* IDENTIFIERS */

  <IDENTIFIER: <LETTER> ( <LETTER> | <DIGIT> )* >
| <DOT_DELIMINATED_REF: <IDENTIFIER> ( "." <IDENTIFIER> )* >
| <QUOTED_DELIMINATED_REF: <QUOTED_VARIABLE> ( "." <QUOTED_VARIABLE> )* >
| <OBJECT_ARRAY_REF: <DOT_DELIMINATED_REF> "[]" >
| <CTALIAS: <IDENTIFIER> >
| <GLOBVARIABLE: <DOT_DELIMINATED_REF> ".*" >
| <QUOTEDGLOBVARIABLE: <QUOTED_DELIMINATED_REF> ".*" >
| <NAMED_PARAMETER: <NAMED_PARAMETER_PREFIX> <IDENTIFIER> >
| <PARAMETER_REF: "?" >
| <NAMED_PARAMETER_PREFIX: "@" >
| <VARIABLE_REF: ":" <IDENTIFIER> >

| <#LETTER: ["a"-"z", "A"-"Z", "_"] >
| <#DIGIT: ["0"-"9"]>

}

TOKEN_MGR_DECLS: {
	public List<Token> tokenHistory = new List<Token>();

	void CommonTokenAction(Token token) {
		tokenHistory.Add(token);
	}
}


// Parses a single expression.  Useed in 'com.mckoi.database.Expression.parse' method.
Expression ParseExpression() :
{ Expression exp;
}
{
  exp = DoExpression() <EOF>
  
  { return exp; }
}

IList<StatementTree> StatementList() :
{ List<StatementTree> list = new List<StatementTree>();
  StatementTree ob;
}
{
  ( ob = Statement() { list.Add(ob); }
    ( ob = Statement() { list.Add(ob); } )* )

  { return list.AsReadOnly(); }
}

// Statement that ends with a ';'
StatementTree Statement() :
{ StatementTree ob; }
{
  (
    (   ob=Select()
      | ob=Update()
      | ob=Alter()
      | ob=Compact()
      | ob=Create()
      | ob=Drop()
      | ob=Delete()
      | ob=Insert()
      | ob=Describe()
      | ob=Show()
      | ob=Call()

      | ob=Declare()
      | ob=Fetch()

      | ob=Open()
      | ob=Close()

      | ob=Grant()
      | ob=Revoke()
      
      | ob=Commit()
      | ob=Rollback()

      | ob=Set()
      
      | ob=ShutDown()
    )
	( <EOF> | <SEMICOLUMN> )
  )
  
  { return ob; }
}
  
// All statements that start with <CREATE>
StatementTree Create() :
{ StatementTree ob;
}
{
  (  <CREATE> 
    (   ob=CreateTable()
      | ob=CreateTrigger()
      | ob=CreateFunction()
      | ob=CreateIndex()
      | ob=CreateSchema()
      | ob=CreateSequence()
      | ob=CreateUser()
      | ob=CreateView()
      | ob=CreateType()
    )
  )
  
  { return ob; }
}  


// All statements that start with <DROP>
StatementTree Drop() :
{ StatementTree ob;
}
{
  (  <DROP>
    (   ob=DropTable()
      | ob=DropTrigger()
      | ob=DropFunction()
      | ob=DropIndex()
      | ob=DropSchema()
      | ob=DropSequence()
      | ob=DropUser()
      | ob=DropView()
    )
  )
  
  { return ob; }
}


StatementTree Declare() :
{ StatementTree ob; }
{
  (  [ <DECLARE> ]
     ( 
       ob=DeclareCursor() |
	   ob=DeclareVariable()
     )
  )
  
  { return ob; }
}

StatementTree DeclareVariable() :
{ StatementTree ob = new StatementTree(typeof(DeclareVariableStatement));
  Token name;
  TType type;
  Expression default_value = null;
  bool constant = false, not_null = false;
}
{
  name = SQLIdentifier() [ <CONSTANT> { constant = true; } ]
   type = GetTType()
  [ <NOT> <NULL_LITERAL> { not_null = true; } ]
  [ <ASSIGNMENT> default_value = DoExpression() ]

  { ob.SetValue("name", name.image);
    ob.SetValue("type", type);
    ob.SetValue("constant", constant);
    ob.SetValue("not_null", not_null);
    ob.SetValue("default", default_value);
    return ob; 
  }
}

StatementTree Open() :
{ StatementTree ob; }
{
  (  <OPEN>
     ( ob=OpenCursor() )
  )
  { return ob; }
}

StatementTree Close() :
{ StatementTree ob; }
{
  (  <CLOSE>
     ( ob=CloseCursor() )
  )
  { return ob; }
}


StatementTree Select() :
{ StatementTree cmd = new StatementTree(typeof(SelectStatement));
  TableSelectExpression table_expr;
  List<ByColumn> order_by = new List<ByColumn>();
}
{
  ( table_expr = GetTableSelectExpression()
    [ <ORDERBY> SelectOrderByList(order_by) ]
  )

  { cmd.SetValue("table_expression", table_expr);
    cmd.SetValue("order_by", order_by);
    return cmd; }
}


StatementTree Update() :
{ StatementTree cmd = new StatementTree(typeof(UpdateTableStatement));
  String table_name;
  List<Assignment> assignments = new List<Assignment>();
  SearchExpression where_clause = null;
  int limit = -1;
  bool from_cursor = false;
  string cursor_name = null;
}
{
  ( <UPDATE> table_name = TableName() <SET> AssignmentList(assignments)
        [ <WHERE>
          ( <CURRENTOF> cursor_name = TableName() { from_cursor = true; } |
            where_clause = ConditionsExpression() )
        ]
        [ <LIMIT> limit = PositiveIntegerConstant() ] 
  )

  { cmd.SetValue("table_name", table_name);
    cmd.SetValue("assignments", assignments);
    cmd.SetValue("where_clause", where_clause);
    cmd.SetValue("from_cursor", from_cursor);
    cmd.SetValue("cursor_name", cursor_name);
    cmd.SetValue("limit", limit);
    return cmd; }
}

StatementTree Alter() :
{ StatementTree cmd;
}
{
  (   <ALTER>
    (   cmd=AlterTable()
      | cmd=AlterUser()
    )
  )
  
  { return cmd; }
}


StatementTree AlterTable() :
{ StatementTree cmd = new StatementTree(typeof(AlterTableStatement));
  String table_name;
  AlterTableAction action;
  StatementTree create_statement;
}
{
  (   <TABLE> table_name=TableName() action=GetAlterTableAction()
      { cmd.SetValue("table_name", table_name);
        cmd.SetValue("alter_action", action);
      }

    | <CREATE> create_statement = CreateTable()
      { cmd.SetValue("create_statement", create_statement); }
        
  )

  { return cmd; }
}


StatementTree Compact() :
{ StatementTree cmd = new StatementTree(typeof(CompactStatement));
  String table_name;
}
{
  (    <COMPACT> <TABLE> table_name=TableName()
  )
  
  { cmd.SetValue("table_name", table_name);
    return cmd; }
}


StatementTree CreateTable() :
{ StatementTree cmd = new StatementTree(typeof(CreateTableStatement));
  bool temporary = false;
  bool only_if_not_exists = false;
  String table_name;
  ArrayList column_list = new ArrayList();
  ArrayList constraint_list = new ArrayList();
  Expression check_expression;
}
{
  (  [ <TEMPORARY> { temporary = true; } ]
       <TABLE>
     [ <IF> <NOT> <EXISTS> { only_if_not_exists = true; } ]
       table_name = TableName()
       ColumnDeclarationList(column_list, constraint_list)
     [ <CHECK> check_expression = DoExpression()
            { constraint_list.Add(SqlConstraint.Check(check_expression)); }
     ]

//     [ CreateOptions(statement) ]
//     [ <AS> statement.select = Select() ]
  )
  
  { cmd.SetValue("temporary", temporary);
    cmd.SetValue("only_if_not_exists", only_if_not_exists);
    cmd.SetValue("table_name", table_name);
    cmd.SetValue("column_list", column_list);
    cmd.SetValue("constraint_list", constraint_list);
    return cmd; }
  
}


StatementTree CreateTrigger() :
{ StatementTree cmd = new StatementTree(typeof(CreateTriggerStatement));
  bool callback;
  String trigger_name;
  ArrayList trigger_types = new ArrayList();
  String table_name;
  String before_after;
  String procedure_name;
  Expression[] procedure_args;
}
{
  (
    ( <CALLBACK> <TRIGGER> trigger_name = TriggerName()
        TriggerTypes(trigger_types)
        <ON> table_name = TableName()
    )
    { cmd.SetValue("type", "callback_trigger");
    }

  | ( <TRIGGER> trigger_name = TriggerName()
        before_after = BeforeOrAfter()
        TriggerTypes(trigger_types)
        <ON> table_name = TableName()
        <FOR> <EACH> <ROW> <EXECUTE> <PROCEDURE>
        procedure_name = FunctionName() "(" procedure_args = ExpressionList() ")"
    )
    { cmd.SetValue("type", "procedure_trigger");
      cmd.SetValue("before_after", before_after);
      cmd.SetValue("procedure_name", procedure_name);
      cmd.SetValue("procedure_args", procedure_args);
    }
    
  )
      
  { cmd.SetValue("trigger_name", trigger_name);
    cmd.SetValue("trigger_types", trigger_types);
    cmd.SetValue("table_name", table_name);
    return cmd; }

}  


StatementTree DropTrigger() :
{ StatementTree cmd = new StatementTree(typeof(DropTriggerStatement));
  String trigger_name;
  String type = null;
}
{
    (   <CALLBACK> <TRIGGER> trigger_name = TriggerName() { type = "callback_trigger"; } )
  | (   <TRIGGER> trigger_name = TriggerName() { type = "procedure_trigger"; } )
  
  { cmd.SetValue("trigger_name", trigger_name);
    cmd.SetValue("type", type);
    return cmd; }
}  


StatementTree CreateFunction() :
{ StatementTree cmd = new StatementTree(typeof(CreateFunctionStatement));
  String function_name;
  ArrayList arg_names = new ArrayList();
  ArrayList arg_types = new ArrayList();
  Token loc_name;
  TType return_type = null;
}
{
  (   <FUNCTION> function_name = FunctionName()
      "(" ProcParameterList(arg_names, arg_types) ")"
      [ <RETURNS> return_type = GetTType() ]
      <AS> <EXTERNAL> loc_name = <STRING_LITERAL>
  )

  { cmd.SetValue("function_name", function_name);
    cmd.SetValue("arg_names", arg_names);
    cmd.SetValue("arg_types", arg_types);
    // Note that 'location_name' will be a TObject
    cmd.SetValue("location_name",
                  Parser.Util.ToParamObject(loc_name, case_insensitive_identifiers));
    cmd.SetValue("return_type", return_type);
    return cmd;
  }
}  


StatementTree DropFunction() :
{ StatementTree cmd = new StatementTree(typeof(DropFunctionStatement));
  String function_name;
}
{
  (   <FUNCTION> function_name = FunctionName() )

  { cmd.SetValue("function_name", function_name);
    return cmd;
  }
}  


StatementTree CreateSchema() :
{ StatementTree cmd = new StatementTree(typeof(CreateSchemaStatement));
  cmd.SetValue("type", "create");
  String schema_name;
}
{
  (   <SCHEMA> schema_name = SchemaName() )
  
  { cmd.SetValue("schema_name", schema_name);
    return cmd; }
}


StatementTree DropSchema() :
{ StatementTree cmd = new StatementTree(typeof(DropSchemaStatement));
  cmd.SetValue("type", "drop");
  String schema_name;
}
{
  (   <SCHEMA> schema_name = SchemaName() )
  
  { cmd.SetValue("schema_name", schema_name);
    return cmd; }
}


StatementTree CreateView() :
{ StatementTree cmd = new StatementTree(typeof(CreateViewStatement));
  String view_name;
  TableSelectExpression select_cmd;
  ArrayList col_list = new ArrayList();
}
{
  (   <VIEW> view_name = TableName() [ "(" BasicColumnList(col_list) ")" ]
        <AS> select_cmd = GetTableSelectExpression() )

  { cmd.SetValue("view_name", view_name);
    cmd.SetValue("column_list", col_list);
    cmd.SetValue("select_expression", select_cmd);
    return cmd; }
}

StatementTree DropView() :
{ StatementTree cmd = new StatementTree(typeof(DropViewStatement));
  String view_name;
}
{
  (   <VIEW> view_name = TableName() )
  
  { cmd.SetValue("view_name", view_name);
    return cmd;
  }
}


StatementTree CreateIndex() :
{
  StatementTree cmd = new StatementTree(typeof(NoOpStatement));
}
{
  (   [<UNIQUE> ] <INDEX> IndexName() <ON>
        TableName() "(" BasicColumnList(new ArrayList()) ")" )
  
  { return cmd; }
}


StatementTree DropTable() :
{ StatementTree cmd = new StatementTree(typeof(DropTableStatement));
  bool only_if_exists = false;
  String table_name;
  ArrayList table_list = new ArrayList();
}
{
  (   <TABLE>
         [ <IF> <EXISTS> { only_if_exists = true; } ]
         table_name = TableName() { table_list.Add(table_name); }
           ( "," table_name = TableName() { table_list.Add(table_name); } )*
  )
  
  { cmd.SetValue("only_if_exists", only_if_exists);
    cmd.SetValue("table_list", table_list);
    return cmd; }
  
}


StatementTree DropIndex() :
{
  StatementTree cmd = new StatementTree(typeof(NoOpStatement));
}
{
  (   <INDEX> IndexName() <ON> TableName()
  )
  
  { return cmd; }

}


StatementTree Call() :
{
  StatementTree cmd = new StatementTree(typeof(CallStatement));
  String proc_name;
  Expression[] args = null;
}
{
  <CALL> proc_name = ProcedureName() "(" args=ExpressionList() ")"
  { cmd.SetValue("proc_name", proc_name);
    cmd.SetValue("args", args);
    return cmd;
  }
}


StatementTree CreateSequence() :
{
  StatementTree cmd = new StatementTree(typeof(CreateSequenceStatement));
  String seq_name;
  Expression v;
}
{
  <SEQUENCE> seq_name=SequenceName() { cmd.SetValue("seq_name", seq_name); }
  [ <INCREMENT> v = DoExpression() { cmd.SetValue("increment", v); } ]
  [ <MINVALUE> v = DoExpression() { cmd.SetValue("min_value", v); } ]
  [ <MAXVALUE> v = DoExpression() { cmd.SetValue("max_value", v); } ]
  [ <START> v = DoExpression() { cmd.SetValue("start", v); } ]
  [ <CACHE> v = DoExpression() { cmd.SetValue("cache", v); } ]
  [ <CYCLE> { cmd.SetValue("cycle", "yes"); } ]
  
  { return cmd; }
}
 
StatementTree DropSequence() :
{
  StatementTree cmd = new StatementTree(typeof(DropSequenceStatement));
  String seq_name;
}
{
  <SEQUENCE> seq_name=SequenceName() { cmd.SetValue("seq_name", seq_name); }
  
  { return cmd; }
}


StatementTree CreateUser() :
{
  StatementTree cmd = new StatementTree(typeof(CreateUserStatement));
}
{
  <USER> UserManagerCommand(cmd)
  
  { return cmd; }
}

StatementTree AlterUser() :
{
  StatementTree cmd = new StatementTree(typeof(AlterUserStatement));
}
{
  <USER> UserManagerCommand(cmd)
  
  { return cmd; }
}

StatementTree DropUser() :
{
  StatementTree cmd = new StatementTree(typeof(DropUserStatement));
  String username;
}
{ 
  <USER> username = UserName()
  
  { cmd.SetValue("username", username);
    return cmd; }
}

void UserManagerCommand(StatementTree cmd) :
{
  String username;
  Expression password_exp;
  Expression[] groups_list = null;
  String lock_status = null;
}
{
  (   username = UserName()
      <SET> <PASSWORD> password_exp=DoExpression()
      [ LOOKAHEAD(2) <SET> <GROUPS> groups_list=ExpressionList() ]
      [ <SET> <ACCOUNT> ( <LOCK> { lock_status="LOCK"; } | <UNLOCK> { lock_status="UNLOCK"; } ) ]
  )
  
  { cmd.SetValue("username", username);
    cmd.SetValue("password_expression", password_exp);
    cmd.SetValue("groups_list", groups_list);
    cmd.SetValue("lock_status", lock_status);
  }
}


StatementTree DeclareCursor() :
{
  Token name;
  bool scrollable = false, update = false, insensitive = false;
  TableSelectExpression select_expr;
  List<ByColumn> order_by = new List<ByColumn>();
}
{
  <CURSOR> name = SQLIdentifier() [ <INSENSITIVE> { insensitive = true;} ] 
     [ <SCROLL> { scrollable=true; } ]
     <FOR> select_expr = GetTableSelectExpression()
     [ <ORDERBY> SelectOrderByList(order_by) ]
     [ <FOR> ( <READONLY> | <UPDATE> { update = true; } ) ]

  { StatementTree ob = new StatementTree(typeof(DeclareCursorStatement));
    ob.SetValue("name", name.image);
    ob.SetValue("scrollable", scrollable);
    ob.SetValue("insensitive", insensitive);
    ob.SetValue("update", update);
    ob.SetValue("select_expression", select_expr);
    ob.SetValue("order_by", order_by);
    return ob;
  }
}

StatementTree OpenCursor() :
{ Token name; }
{
  name = SQLIdentifier()

  { StatementTree ob = new StatementTree(typeof(OpenCursorStatement));
    ob.SetValue("name", name.image);
    return ob;
  }
}

StatementTree CloseCursor() :
{ Token name; }
{
  name = SQLIdentifier()

  { StatementTree ob = new StatementTree(typeof(CloseCursorStatement));
    ob.SetValue("name", name.image);
    return ob;
  }
}

StatementTree Fetch() :
{
  Token cursor_name;
  CursorFetch fetch = new CursorFetch();
}
{
  <FETCH> [
    [ <NEXT> { fetch.Orientation = FetchOrientation.Next; } |
      <PRIOR> { fetch.Orientation = FetchOrientation.Prior; } |
      <FIRST> { fetch.Orientation = FetchOrientation.First; } |
      <LAST> { fetch.Orientation = FetchOrientation.Last; } |
      <RELATIVE> { fetch.Orientation = FetchOrientation.Relative; } 
        fetch.Offset = DoExpression() |
      <ABSOLUTE> { fetch.Orientation = FetchOrientation.Absolute; }
        fetch.Offset = DoExpression()
    ]
      
    <FROM>
  ]

  cursor_name = SQLIdentifier()

  [ <INTO> GetIntoClause(fetch.Into) ]

  { StatementTree ob = new StatementTree(typeof(FetchStatement));
    ob.SetValue("name", cursor_name.image);
    ob.SetValue("fetch", fetch);
    return ob;
  }
}


StatementTree Delete() :
{ StatementTree cmd = new StatementTree(typeof(DeleteStatement));
  String table_name;
  SearchExpression where_clause = null;
  int limit = -1;
  string cursor_name = null;
  bool from_cursor = false;
}
{

  ( <DELETE> <FROM> table_name = TableName()
        [ <WHERE> 
          ( <CURRENTOF> cursor_name = TableName() { from_cursor = true; } | 
            where_clause = ConditionsExpression() )
        ]
        [ <LIMIT> limit = PositiveIntegerConstant() ] 
  )

  { cmd.SetValue("table_name", table_name);
    cmd.SetValue("where_clause", where_clause);
    cmd.SetValue("limit", limit);
    cmd.SetValue("from_cursor", from_cursor);
    cmd.SetValue("cursor_name", cursor_name);
    return cmd; }

}


StatementTree Insert() :
{ StatementTree cmd = new StatementTree(typeof(InsertStatement));
  String table_name;
  ArrayList col_list = new ArrayList();
  ArrayList data_list = new ArrayList(); // ( Array of Expression[] )
  StatementTree select = null;
  List<Assignment> assignments = new List<Assignment>();
  String type;
}
{

  ( <INSERT> [ <INTO> ] table_name = TableName()
    (   [ "(" BasicColumnList(col_list) ")" ]
           (   <VALUES> InsertDataList(data_list)  { type = "from_values"; }
             | select = Select()                   { type = "from_select"; }
           )
      | <SET> AssignmentList(assignments)          { type = "from_set"; }
    )
  )

  { cmd.SetValue("table_name", table_name);
    cmd.SetValue("col_list", col_list);
    cmd.SetValue("data_list", data_list);
    cmd.SetValue("select", select);
    cmd.SetValue("assignments", assignments);
    cmd.SetValue("type", type);
    return cmd; }

}


StatementTree Describe() :
{ StatementTree cmd = new StatementTree(typeof(ShowStatement));
  cmd.SetValue("show", "describe_table");
  String table_name;
}
{

  ( <DESCRIBE> table_name = TableName()
  )
  
  { cmd.SetValue("table_name", table_name);
    cmd.SetValue("where_clause", null);
    return cmd; }

}


StatementTree Show() :
{ StatementTree cmd = new StatementTree(typeof(ShowStatement));
  Expression[] args = null;
  SearchExpression where_clause = null;
  Token t;
}
{
  ( <SHOW> 
      (   t=<IDENTIFIER>
        | t=<SCHEMA>
      )
      [ "(" args=ExpressionList() ")" ]
      [ <WHERE> where_clause = ConditionsExpression() ] )
  
  { cmd.SetValue("show", t.image);
    cmd.SetValue("args", args);
    cmd.SetValue("where_clause", where_clause);
    return cmd; }

}


StatementTree Grant() :
{ StatementTree cmd = new StatementTree(typeof(GrantStatement));
  ArrayList priv_list = new ArrayList();
  String priv_object;
  ArrayList grant_to;
  bool grant_option = false;
}
{
  ( <GRANT> PrivList(priv_list) <ON> priv_object=PrivObject()
    <TO> grant_to=UserNameList(new ArrayList())
    [ <WITH> <GRANT> <OPTION> { grant_option = true; } ]
  )
  
  { cmd.SetValue("priv_list", priv_list);
    cmd.SetValue("priv_object", priv_object);
    cmd.SetValue("users", grant_to);
    cmd.SetValue("grant_option", grant_option);
    return cmd;
  }
}


StatementTree Revoke() :
{ StatementTree cmd = new StatementTree(typeof(RevokeStatement));
  ArrayList priv_list = new ArrayList();
  String priv_object;
  ArrayList revoke_from;
  bool revoke_grant_option = false;
}
{
  ( <REVOKE> [ <GRANT> <OPTION> <FOR> { revoke_grant_option = true; } ]
    PrivList(priv_list) <ON> priv_object=PrivObject()
    <FROM> revoke_from=UserNameList(new ArrayList())
  )
  
  { cmd.SetValue("priv_list", priv_list);
    cmd.SetValue("priv_object", priv_object);
    cmd.SetValue("users", revoke_from);
    cmd.SetValue("grant_option", revoke_grant_option);
    return cmd;
  }
}

StatementTree CreateType() : 
{ StatementTree cmd = new StatementTree(typeof(NoOpStatement)); 
  string type_name;
  string parent_type = null;
  Token ext_parent_type = null;
  bool external = false;
  bool final = false;
  ArrayList members = new ArrayList(); }
{
   ( <TYPE> type_name =TableName() <AS> <OBJECT>
     [ <UNDER> parent_type=TableName() ]
     [ <EXTERNAL> <NAME> ext_parent_type=<STRING_LITERAL> 
       <LANGUAGE> <CSHARP> { external =true; } ]
     TypeMemberDeclarations(members)
     [ <NOT> <FINAL> | <FINAL> { final = true; } ]
   )

  { cmd.SetValue("type_name", type_name);
    if (external)cmd.SetValue("parent_type", parent_type);
    else cmd.SetValue("parent_type", ext_parent_type.image);
    cmd.SetValue("external", external);
    cmd.SetValue("final", final);
    cmd.SetValue("members", members);
    return cmd; }
}

StatementTree DropType() :
{ StatementTree cmd = new StatementTree(typeof(NoOpStatement));
  string type_name;
  ArrayList type_list = new ArrayList(); }
{
  ( <TYPE> type_name = TableName() { type_list.Add(type_name); }
        ( "," type_name = TableName() { type_list.Add(type_name); } )* )

  { cmd.SetValue("type_list", type_list);
    return cmd; }
}

void TypeMemberDeclarations(ArrayList members) :
{ }
{
  "(" TypeAttributeOrFunctionDeclaration(members)
     ( "," TypeAttributeOrFunctionDeclaration(members) )*
  ")"
}

void TypeAttributeOrFunctionDeclaration(ArrayList members) :
{ }
{
   TypeAttributeDeclaration(members) |
   TypeFunctionDeclaration(members)
}

void TypeFunctionDeclaration(ArrayList members) :
{ ArrayList arg_names = new ArrayList(); 
  ArrayList arg_types = new ArrayList(); }
{
    ( <FUNCTION> FunctionName()
    "(" ProcParameterList(arg_names, arg_types) ")"
    [ <RETURNS> GetTType() ]
  )
}

void TypeAttributeDeclaration(ArrayList members) :
{ Token name; 
  TType type;
  bool not_null = false; }
{
  ( name = SQLIdentifier() type=GetTType() 
    [ <NOT> <NULL_LITERAL> { not_null=true; } ] )
  { members.Add(new SqlTypeAttribute(name.image, type, not_null)); }
}

StatementTree Commit() :
{ 
  StatementTree cmd = new StatementTree(typeof(CommitStatement)); 
}
{
   <COMMIT>

  { return cmd; }
}

StatementTree Rollback() :
{
 StatementTree cmd = new StatementTree(typeof(RollbackStatement));
}
{
  <ROLLBACK>
  { return cmd; }
}

StatementTree Set() :
{ StatementTree cmd = new StatementTree(typeof(SetStatement));
  Token t1;
  String value;
  Expression exp;
  String name;
}
{
  <SET>
  (   t1=<IDENTIFIER> <ASSIGNMENT> exp=DoExpression()
        { cmd.SetValue("type", "VARSET");
          cmd.SetValue("var_name", t1.image);
          cmd.SetValue("exp", exp); }
    | <TRANSACTIONISOLATIONLEVEL> (  t1=<SERIALIZABLE> )
        { cmd.SetValue("type", "ISOLATIONSET");
          cmd.SetValue("var_name", "TRANSACTION ISOLATION LEVEL");
          cmd.SetValue("value", t1.image); }
    | <AUTOCOMMIT> ( t1=<ON> | t1=<IDENTIFIER> )
        { cmd.SetValue("type", "AUTOCOMMIT");
          cmd.SetValue("value", t1.image); }
    | <SCHEMA> name=SchemaName()
        { cmd.SetValue("type", "SCHEMA");
          cmd.SetValue("value", name); }
  )
  
  { return cmd; }
}



StatementTree ShutDown() :
{ StatementTree cmd = new StatementTree(typeof(ShutdownStatement));
}
{

  <SHUTDOWN>
  
  { cmd.SetValue("command", "shutdown");
    return cmd; }
}



// ----------

String TriggerType() :
{ }
{
  (   <INSERT> { return "insert"; }
    | <DELETE> { return "delete"; }
    | <UPDATE> { return "update"; }
  )
}

String BeforeOrAfter() :
{ }
{
  (   <BEFORE> { return "before"; }
    | <AFTER> { return "after"; }
  )
}

// A list of triggered actions separated by 'OR' delimination, for example,
// INSERT OR DELETE OR UPDATE
void TriggerTypes(ArrayList list) :
{ String trig_type;
}
{
  trig_type = TriggerType() { list.Add(trig_type); }
    ( <OR> trig_type = TriggerType() { list.Add(trig_type); } )*
}

// A priv object
// Note we add a 2 character prefix to the priv object for future enhancements.
// In the future an object may be something other than a table.
String PrivObject() :
{ String table_name;
  String schema_name;
}
{
  (   [ <TABLE> ] table_name=TableName() { return "T:" + table_name; }
    | <SCHEMA> schema_name=SchemaName() { return "S:" + schema_name; }
  )
}

// A list of privs
ArrayList PrivList(ArrayList list) :
{
}
{
        PrivListItem(list)
  ( "," PrivListItem(list) )*
  
  { return list; }
}

// Adds an item in a priv list
void PrivListItem(ArrayList list) :
{ Token t;
}
{
  (   t=<SELECT> | t=<INSERT> | t=<UPDATE> | t=<DELETE>
    | t=<REFERENCES> | t=<USAGE> | t=<ALL> [ <PRIVILEGES> ]
  )
  
  { list.Add(t.image); }
}


// A table expression 
TableSelectExpression GetTableSelectExpression() :
{ TableSelectExpression table_expr = new TableSelectExpression();
  CompositeFunction composite = CompositeFunction.None;
  bool is_all = false;
  TableSelectExpression next_composite_expression;
}
{
  ( <SELECT>
        ( 
          LOOKAHEAD(<IDENTITY>) <IDENTITY> { table_expr.Columns.Add(Sql.SelectColumn.Identity); } |
          [ table_expr.Distinct = SetQuantifier() ] 
          SelectColumnList(table_expr.Columns) 
        )
        [ <INTO> GetIntoClause(table_expr.Into) ]
        [ <FROM> SelectTableList(table_expr.From) ]
        [ <WHERE> table_expr.Where = ConditionsExpression() ]

        [ <GROUPBY> SelectGroupByList(table_expr.GroupBy)
          [ <GROUPMAX> table_expr.GroupMax = GroupMaxColumn() ] 
          [ <HAVING> table_expr.Having = ConditionsExpression() ] ]

        [ composite = GetComposite() [ <ALL> { is_all = true; } ]
          next_composite_expression = GetTableSelectExpression()
          { table_expr.ChainComposite(next_composite_expression, composite, is_all); } 
        ]
  )
  { return table_expr; }
}

AlterTableAction GetAlterTableAction() :
{ String col_name, con_name;
  SqlColumn column;
  SqlConstraint constraint_def;
  Expression default_exp;
  AlterTableActionType actionType = AlterTableActionType.AddColumn;
  ArrayList elements = new ArrayList();
}
{
  (   <SQLADD>
      (   [ <SQLCOLUMN> ] column=ColumnDefinition()
          { actionType = AlterTableActionType.AddColumn;
            elements.Add(column);
          }
        | constraint_def=TableConstraintDefinition()
          { actionType = AlterTableActionType.AddConstraint;
            elements.Add(constraint_def);
          }
      )
    | <ALTER> [ <SQLCOLUMN> ] col_name=ColumnName()
      (   <SET> default_exp=DoExpression()
          { actionType = AlterTableActionType.SetDefault;
            elements.Add(col_name);
            elements.Add(default_exp);
          }
        | <DROP> <SQLDEFAULT>
          { actionType= AlterTableActionType.DropDefault;
            elements.Add(col_name);
          }
      )
    | <DROP>
      (   [ <SQLCOLUMN> ] col_name=ColumnName()
          { actionType = AlterTableActionType.DropColumn;
            elements.Add(col_name);
          }
        | <CONSTRAINT> con_name=ConstraintName()
          { actionType = AlterTableActionType.DropConstraint;
            elements.Add(con_name);
          }
        | <PRIMARY> <KEY>
          { actionType = AlterTableActionType.DropPrimaryKey; }
      )
  )
  
  { AlterTableAction action = new AlterTableAction(actionType);
	action.AddElements(elements);
	return action; }
}

void GetIntoClause(SelectIntoClause into) :
{ string table_name = null;  
  Token var_ref = null; }
{
   ( table_name = TableName() { into.SetTableName(table_name); } |
     var_ref = <VARIABLE_REF> { into.AddElement(CreateVariableRef(var_ref.image)); }
     ("," var_ref = <VARIABLE_REF> { into.AddElement(CreateVariableRef(var_ref.image)); } )*
   )
}

// An element to insert, either an expression or DEFAULT for the default
// element.
Object InsertElement() :
{ Expression e;
}
{
  (   <SQLDEFAULT>        { return "DEFAULT"; }
    | e = DoExpression()  { return e; }
  )
}

ArrayList InsertExpressionList() :
{ ArrayList list = new ArrayList();
  Object elem;
}
{
  [ elem = InsertElement() { list.Add(elem); }
    ( "," elem = InsertElement() { list.Add(elem); }  )*
  ]

  { return list; }
}  

// The list of columns to insert formatted as; eg.  (9, 4), (3, 2), (9, 9), ....
void InsertDataList(ArrayList data_list) :
{ ArrayList insert_vals;
}
{

    "(" insert_vals = InsertExpressionList() ")" { data_list.Add(insert_vals); }
  ( "," "(" insert_vals = InsertExpressionList() ")" { data_list.Add(insert_vals); } )*

}

// Returning true means distinct, false means all.
bool SetQuantifier() :
{}
{  ( <DISTINCT> { return true; } |
     <ALL>      { return false; } )
}
     

void SelectColumnList(IList list) :
{ SelectColumn col;
}
{
    col = SelectColumn() { list.Add(col); }
  ( "," col = SelectColumn() { list.Add(col); } )* 

}

SelectColumn SelectColumn() :
{ String alias = null;
  Token t;
  Expression exp;
}
{ 
  (   exp = DoExpression() [ <AS> ] [ alias=TableAliasName() ]
    | <STAR> { return Sql.SelectColumn.Glob("*"); }
    | t = <GLOBVARIABLE> { return Sql.SelectColumn.Glob(CaseCheck(t.image)); }
    | t = <QUOTEDGLOBVARIABLE> { return Sql.SelectColumn.Glob(CaseCheck(Parser.Util.AsNonQuotedRef(t))); }
  )
  { return new SelectColumn(exp, alias); }
}

void SelectGroupByList(IList list) :
{ ByColumn col;
  Expression exp;
}
{
    exp = DoExpression() { col = new ByColumn(exp);
                           list.Add(col); }
  ( "," exp = DoExpression() { col = new ByColumn(exp);
                               list.Add(col); } )*
  
}

/**
 * NOTE: This is an extension, allows for us to specify a column to return the
 *  max value for each row representing a group.
 */
VariableName GroupMaxColumn() :
{ VariableName var; }
{
  var = ColumnNameVariable()
  { return var; }
}



void SelectOrderByList(IList list) :
{ ByColumn col;
  Expression exp;
  bool ascending = true;
}
{
    exp = DoExpression() [ ascending=OrderingSpec() ]
                         { col = new ByColumn(exp, ascending);
                           list.Add(col); }
  ( "," exp = DoExpression() { ascending=true; } [ ascending=OrderingSpec() ]
                         { col = new ByColumn(exp, ascending);
                           list.Add(col); } )*
  
}

bool OrderingSpec() :
{}
{
  ( <ASC> { return true; } | <DESC> { return false; } )
  
  { return true; }
}


void TableDeclaration(FromClause from_clause) :
{ String table=null, declare_as = null;
  TableSelectExpression select_stmt = null;
}
{

  ( ( table=TableName() | "(" select_stmt=GetTableSelectExpression() ")" )
    [ [ <AS> ] declare_as=TableName() ] )
  
  { from_clause.AddTableDeclaration(table, select_stmt, declare_as); }

}

void SelectTableList(FromClause from_clause) :
{}
{
  TableDeclaration(from_clause) [ FromClauseJoin(from_clause) ]
}

void FromClauseJoin(FromClause from_clause) :
{ Expression on_expression; }
{ 
  (
      (
        ","
        { from_clause.AddJoin(JoinType.Inner);}
      ) [ SelectTableList(from_clause) ]
    | (
        [ <INNER> ] <JOIN> TableDeclaration(from_clause) <ON> on_expression=DoExpression()
        { from_clause.AddPreviousJoin(JoinType.Inner, on_expression); }
      ) [ FromClauseJoin(from_clause) ]
    | (
        <LEFT> [<OUTER>] <JOIN> TableDeclaration(from_clause) <ON> on_expression=DoExpression()
        { from_clause.AddPreviousJoin(JoinType.Left, on_expression); }
      ) [ FromClauseJoin(from_clause) ]
    | (
        <RIGHT> [<OUTER>] <JOIN> TableDeclaration(from_clause) <ON> on_expression=DoExpression()
        { from_clause.AddPreviousJoin(JoinType.Right, on_expression); }
      ) [ FromClauseJoin(from_clause) ]
  )

}






// A list of parameters in a function or procedure declaration.  For example,
// ' p1 NUMERIC, p2 NUMERIC, s1 CHARACTER VARYING '
// First array contains parameter names, and second contains TType representing
// the type.
void ProcParameterList(ArrayList decl_names, ArrayList decl_types) :
{ String name;
  TType type;
}
{
  [ { name = null; }
    ( [ name = ProcArgumentName() ] type = GetTType() ) { decl_names.Add(name);
                                                          decl_types.Add(type); }
    ( ( "," { name = null; }
        [ name = ProcArgumentName() ] type = GetTType() ) { decl_names.Add(name);
                                                            decl_types.Add(type); }
    )*
  ]
}


// The ' set a = (a * 9), b = concat(b, "aa") ' part of the 'update', 'insert' statement
void AssignmentList(IList assignment_list) :
{ String column;
  Expression exp;
}
{
  ( column=ColumnName() <ASSIGNMENT> exp=DoExpression()
    { assignment_list.Add(new Assignment(VariableName.Resolve(column), exp)); }
    [ "," AssignmentList(assignment_list) ]   
  )
}

// Parses a list of column declarations.  eg. ' id NUMERIC(5, 20), number VARCHAR(90), ... '
// and also any constraints.
void ColumnDeclarationList(ArrayList column_list, ArrayList constraint_list) :
{
}
{
  "(" ColumnOrConstraintDefinition(column_list, constraint_list)
      ( "," ColumnOrConstraintDefinition(column_list, constraint_list) )*
  ")"
}

void ColumnOrConstraintDefinition(ArrayList column_list, ArrayList constraint_list) :
{ SqlColumn coldef = null;
  SqlConstraint condef = null;
}
{
  (   coldef = ColumnDefinition()           { column_list.Add(coldef); }
    | condef = TableConstraintDefinition()  { constraint_list.Add(condef); }
  )
}

SqlColumn ColumnDefinition() :
{ SqlColumn column = new SqlColumn(true);
  Token t;
  Token col_constraint;
  Expression default_exp;
  String col_name;
}
{
  ( col_name = ColumnName() { column.SetName(col_name); }
    ColumnDataType(column)
    [ <SQLDEFAULT> default_exp = DoExpression() { column.Default = default_exp; } ]
    ( ColumnConstraint(column) )*
    [ ( t=<INDEX_BLIST> | t=<INDEX_NONE> ) { column.SetIndex(t); } ] 
  )
  
  { return column; }

}

// Constraint on a column, eg. 'NOT NULL', 'NULL', 'PRIMARY KEY', 'UNIQUE', etc.
void ColumnConstraint(SqlColumn column) :
{ Token t;
  String table_name;
  ArrayList col_list = new ArrayList();
}
{
    (   <NOT> <NULL_LITERAL>  { column.AddConstraint("NOT NULL"); }
      | <NULL_LITERAL> { column.AddConstraint("NULL"); }
      | <PRIMARY> <KEY> { column.AddConstraint("PRIMARY"); }
      | <UNIQUE> { column.AddConstraint("UNIQUE"); }
    )
    
}


CollationStrength GetCollateStrength() :
{ 
}
{ (   <PRIMARY_STRENGTH>    { return CollationStrength.Primary; }
    | <SECONDARY_STRENGTH>  { return CollationStrength.Secondary; }
    | <TERTIARY_STRENGTH>   { return CollationStrength.Tertiary; }
    | <IDENTICAL_STRENGTH>  { return CollationStrength.Identical; }
  )
}

CollationDecomposition GetCollateDecomposition() :
{ }
{ (   <NO_DECOMPOSITION>        { return CollationDecomposition.None; }
    | <CANONICAL_DECOMPOSITION> { return CollationDecomposition.Canonical; }
    | <FULL_DECOMPOSITION>      { return CollationDecomposition.Full; }
  )
}


SqlType GetStringSQLType() :
{ }
{
    LOOKAHEAD(2) ( <CHARACTER> <VARYING> ) { return SqlType.VarChar; }
  | LOOKAHEAD(3) ( <LONG> <CHARACTER> <VARYING> ) { return SqlType.LongVarChar; }
  | ( <TEXT> | <STRING> | <LONGVARCHAR> ) { return SqlType.LongVarChar; }
  | ( <CHAR> | <CHARACTER> ) { return SqlType.Char; }
  | <VARCHAR> { return SqlType.VarChar; }
  | <CLOB> { return SqlType.Clob; }
}

SqlType GetNumericSQLType() :
{ }
{
    ( <INT> | <INTEGER> ) { return SqlType.Integer; }
  | <TINYINT> { return SqlType.TinyInt; }
  | <SMALLINT> { return SqlType.SmallInt; }
  | <BIGINT> { return SqlType.BigInt; }
  | <FLOAT> { return SqlType.Float; }
  | <REAL> { return SqlType.Real; }
  | <DOUBLE> { return SqlType.Double; }
  | <NUMERIC> { return SqlType.Numeric; }
  | <DECIMAL> { return SqlType.Decimal; }
}

SqlType GetBooleanSQLType() :
{ }
{
  ( <BOOLEAN> | <BIT> ) { return SqlType.Boolean; }
}

SqlType GetDateSQLType() :
{ }
{
    <TIMESTAMP> { return SqlType.TimeStamp; }
  | <TIME> { return SqlType.Time; }
  | <DATE> { return SqlType.Date; }
}

SqlType GetBinarySQLType() :
{ }
{
    LOOKAHEAD(2) ( <BINARY> <VARYING> ) { return SqlType.VarBinary; }
  | LOOKAHEAD(3) ( <LONG> <BINARY> <VARYING> ) { return SqlType.LongVarBinary; }
  | <LONGVARBINARY> { return SqlType.LongVarBinary; }
  | <VARBINARY> { return SqlType.VarBinary; }
  | <BINARY> { return SqlType.Binary; }
  | <BLOB> { return SqlType.Blob; }
}

SqlType GetIntervalSQLType() :
{ SqlType sqlType = SqlType.Interval; }
{
  <INTERVAL>
  [ ( <YEAR> <TO> <MONTH> ) { sqlType = SqlType.YearToMonth; } | 
    ( <DAY> <TO> <SECOND> ) { sqlType = SqlType.DayToSecond; } ]

  { return sqlType; }
}

SqlType GetIdentitySQLType() :
{ }
{
    <IDENTITY> { return SqlType.Identity; }
}

// Parses an SQL type and forms a TType object.  For example, "CHAR(500)" is
// parsed to a TStringType with a maximum size of 500 and lexicographical
// collation.
TType GetTType() :
{ Token t;
  SqlType data_type;
  int size = -1;
  int scale = -1;
  Token class_tok = null;
  CollationStrength strength = CollationStrength.None;
  CollationDecomposition decomposition = CollationDecomposition.None;
  String loc = null;
}
{
  (   <OBJECT> [ "(" ( class_tok=<DOT_DELIMINATED_REF> | class_tok=<OBJECT_ARRAY_REF> ) ")" ]
      { String class_str = "System.Object";
        if (class_tok != null) {
          class_str = class_tok.image;
        }
        return TType.GetObjectType(class_str);
      } 

    | LOOKAHEAD(GetStringSQLType())
      data_type = GetStringSQLType() [ "(" size = PositiveIntegerConstant() ")" ]
      [ <COLLATE> t=<STRING_LITERAL> { loc = ((TObject) Parser.Util.ToParamObject(t, case_insensitive_identifiers)).ToString(); }
         [ strength=GetCollateStrength() ] [ decomposition = GetCollateDecomposition() ] ]
      { return TType.GetStringType(data_type, size, loc, strength, decomposition); }

    | data_type = GetIdentitySQLType() 
      { return TType.GetNumericType(data_type, -1, -1); }

    | data_type = GetNumericSQLType() [ "(" size = PositiveIntegerConstant()
                             [ "," scale = PositiveIntegerConstant() ] ")" ]
      { return TType.GetNumericType(data_type, size, scale); } 
      
    | data_type = GetBooleanSQLType()
      { return TType.GetBooleanType(data_type); }

    | data_type = GetDateSQLType()
      { return TType.GetDateType(data_type); }

    | data_type = GetBinarySQLType() [ "(" size = PositiveIntegerConstant() ")" ]
      { return TType.GetBinaryType(data_type, size); }

    | data_type = GetIntervalSQLType()
      { return TType.GetIntervalType(data_type); }
  )
}

// Data type of a SqlColumn (eg. "varchar(50)", etc)
void ColumnDataType(SqlColumn column) :
{ TType type;
}
{
  type = GetTType() { column.SetType(type); }
}


SqlConstraint TableConstraintDefinition() :
{ SqlConstraint constraint = null;
  ArrayList column_list = new ArrayList();
  ArrayList column_list2 = new ArrayList();
  String constraint_name = null;
  ConstraintAction update_rule = ConstraintAction.NoAction;
  ConstraintAction delete_rule = ConstraintAction.NoAction;
  Expression expression;
  String name;
  String reference_table;
  Token t;
}
{
  ( [ <CONSTRAINT> constraint_name = ConstraintName() ]

    (   <PRIMARY> <KEY> "(" BasicColumnList(column_list) ")" { constraint = SqlConstraint.PrimaryKey((string[])column_list.ToArray(typeof(string))); }
      | <UNIQUE> "(" BasicColumnList(column_list) ")"      { constraint = SqlConstraint.Unique((string[])column_list.ToArray(typeof(string))); }
      | <CHECK> "(" expression = DoExpression() ")"        { constraint = SqlConstraint.Check(expression); }
      | <FOREIGN> <KEY> "(" BasicColumnList(column_list) ")"
        <REFERENCES> reference_table=TableName() [ "(" BasicColumnList(column_list2) ")" ]
        [   LOOKAHEAD(2) ( <ON> <DELETE> delete_rule=ReferentialTrigger()
              [ <ON> <UPDATE> update_rule=ReferentialTrigger() ]
            )
          | ( <ON> <UPDATE> update_rule=ReferentialTrigger()
              [ <ON> <DELETE> delete_rule=ReferentialTrigger() ]
            )
        ]
        { constraint = SqlConstraint.ForeignKey(reference_table, (string[])column_list.ToArray(typeof(string)), (string[])column_list2.ToArray(typeof(string)), delete_rule, update_rule); }
    )

    // Constraint deferrability
    [ ConstraintAttributes(constraint) ]
   
  )
  
  { if (constraint_name != null) constraint.Name = constraint_name;
    return constraint; }

}

ConstraintAction ReferentialTrigger() :
{ Token t;
  ConstraintAction action;
}
{
  (   <NO> <ACTION>                     { action = ConstraintAction.NoAction; }
    | <RESTRICT>                        { action = ConstraintAction.NoAction; }
    | <CASCADE>                         { action = ConstraintAction.Cascade; }
    | LOOKAHEAD(2) <SET> <NULL_LITERAL> { action = ConstraintAction.SetNull; }
    | <SET> <SQLDEFAULT>                { action = ConstraintAction.SetDefault; }
  )

  { return action; }
}

void ConstraintAttributes(SqlConstraint constraint) :
{
}
{
  (   (
        <INITIALLY> (   <DEFERRED>  { constraint.SetInitiallyDeferred(); }
                      | <IMMEDIATE>
                    )

        [   <NOT> <DEFERRABLE>  { constraint.SetNotDeferrable(); }
          | <DEFERRABLE>
        ]
      )
    |
      (
        ( <NOT> <DEFERRABLE>  { constraint.SetNotDeferrable(); } | <DEFERRABLE> )
        [ <INITIALLY> (   <DEFERRED>  { constraint.SetInitiallyDeferred(); }
                        | <IMMEDIATE>
                      )
        ]
      )
  )
}


// A list of column names
ArrayList BasicColumnList(ArrayList list) :
{ String col_name;
}
{
        col_name = ColumnName() { list.Add(col_name); }
  ( "," col_name = ColumnName() { list.Add(col_name); } )*

  { return list; }
}

// A list of user names
ArrayList UserNameList(ArrayList list) :
{ String username;
}
{
        username = UserName() { list.Add(username); }
  ( "," username = UserName() { list.Add(username); } )*

  { return list; }
}



SearchExpression ConditionsExpression() :
{ Expression exp; }
{
  exp = DoExpression()

  { return new SearchExpression(exp); }
}


Expression DoExpression() :
{ Stack stack = new Stack();
  Expression exp = new Expression();
}
{

  expression(exp, stack)

  { expEnd(exp, stack);
    // Normalize the expression (remove any NOT operators)
    Expression normalized_exp = Parser.Util.Normalize(exp);
    normalized_exp.CopyTextFrom(exp);
    return normalized_exp;
  }
}

Expression DoNonBooleanExpression() :
{ Stack stack = new Stack();
  Expression exp = new Expression();
}
{

  nonBooleanExpression(exp, stack)

  { expEnd(exp, stack);
    return exp; }
}


/**
 * Parse an expression.
 */
void expression(Expression exp, Stack stack) :
{ 
}
{

  Operand(exp, stack) ( LOOKAHEAD(2) OpPart(exp, stack) )*

}

/**
 * Parses a non-bool expression.
 */
void nonBooleanExpression(Expression exp, Stack stack) :
{
}
{
  
  Operand(exp, stack)
     ( LOOKAHEAD(2) (  StringOperator(exp, stack)
                     | NumericOperator(exp, stack) ) Operand(exp, stack) )* 

}

void OpPart(Expression exp, Stack stack) :
{ Token t;
//  SelectStatement select;
//  Expression[] exp_arr;
  Expression regex_expression;
  Object regex_ob;
  
}
{

  (   LOOKAHEAD(3) (   BooleanOperator(exp, stack)
                     | NumericOperator(exp, stack)
                     | StringOperator(exp, stack) )
                   Operand(exp, stack)

      // NOTE: Handling regex literals is a horrible horrible hack.  The <REGEX_LITERAL> 
      //   token starts with 'regex /' and the regex string follows.
    | (   <REGEX> { exp.Text.Append(" regex ");
                    expOperator(exp, stack, Operator.Get("regex")); }
                  expression(exp, stack)
                | t=<REGEX_LITERAL>
                  { regex_ob = Parser.Util.ToParamObject(t, case_insensitive_identifiers);
                    exp.Text.Append(" regex " + regex_ob);
                    expOperator(exp, stack, Operator.Get("regex"));
                    exp.AddElement(regex_ob); }
       )

    | LOOKAHEAD(2) SubQueryOperator(exp, stack) SubQueryExpression(exp, stack) 

    | BetweenPredicate(exp, stack) 

  )
  
}  
  

void Operand(Expression exp, Stack stack) :
{ Token t, tt = null;
  RoutineInvoke f;
  Expression[] exp_list;
  String time_fname;
  bool negative = false;
  object param_ob;
  object param_resolve;
  VariableRef variable_ref;
  TableSelectExpression select_expression = null;
  string interval_form = "full";
}
{
  (   "(" { stack.Push(Operator.Get("(")); exp.Text.Append("("); }
        expression(exp, stack) ")" { expEndParen(exp, stack); exp.Text.Append(")"); }
    | t = <PARAMETER_REF>
          { 
            param_resolve = CreateSubstitution(t.image); 
            exp.AddElement(param_resolve);
            exp.Text.Append('?');
          }
    | t = <NAMED_PARAMETER>
          { 
            param_resolve = CreateSubstitution(t.image);
            exp.AddElement(param_resolve);
            exp.Text.Append(t.image);
          }

    | t = <VARIABLE_REF>
          {
            variable_ref = CreateVariableRef(t.image);
            exp.AddElement(variable_ref);
            exp.Text.Append(t.image);
          }

    | LOOKAHEAD(2) <NOT>
      { expOperator(exp, stack, Operator.Get("not"));
        exp.Text.Append(" not ");
      }
      Operand(exp, stack)
          
    | LOOKAHEAD(3) ( f = Function()
          { exp.AddElement(f); exp.Text.Append(f); }
      ) 

// Time values
| ( (   tt=<DATE> { time_fname="DATEOB"; }
      | tt=<TIME> { time_fname="TIMEOB"; } 
      | tt=<TIMESTAMP> { time_fname="TIMESTAMPOB"; }
    )
    t=<STRING_LITERAL>
    { Object param_ob1 = Parser.Util.ToParamObject(t, case_insensitive_identifiers);
      exp_list = new Expression[] { new Expression(param_ob1) };
      f = Parser.Util.ResolveFunctionName(time_fname, exp_list);
      exp.AddElement(f);
      exp.Text.Append(tt.image).Append(" ").Append(t.image);
    }
  )

// Current timestamp
| ( (   tt=<CURRENT_TIMESTAMP> { time_fname="TIMESTAMPOB"; }
      | tt=<CURRENT_TIME>      { time_fname="TIMEOB"; }
      | tt=<CURRENT_DATE>      { time_fname="DATEOB"; }
    )
    { exp_list = new Expression[0];
      f = Parser.Util.ResolveFunctionName(time_fname, exp_list);
      exp.AddElement(f);
      exp.Text.Append(tt.image);
    }
  )

// Current timezone
| (
     tt=<DBTIMEZONE> { time_fname="DBTIMEZONE"; }
     { exp_list = new Expression[0];
       f = Parser.Util.ResolveFunctionName(time_fname, exp_list);
       exp.AddElement(f);
       exp.Text.Append(tt.image);
     }
  )

// Interval conversion
| (
    <INTERVAL> t=<STRING_LITERAL>
    [ <YEAR> { interval_form = "YEAR"; } [ <TO> <MONTH> { interval_form="YEAR TO MONTH"; } ] | 
      <MONTH> { interval_form = "MONTH"; } | 
      <DAY> { interval_form = "DAY"; } [ <TO> <SECOND> { interval_form = "DAY TO SECOND"; } ] | 
      <HOUR> { interval_form = "HOUR"; } | 
      <MINUTE> { interval_form = "MINUTE"; } | 
      <SECOND> { interval_form = "SECOND"; } ]
    { object param_ob2 = Parser.Util.ToParamObject(t, case_insensitive_identifiers);
      exp_list = new Expression[2];
      exp_list[0] = new Expression(param_ob2);
      exp_list[1] = new Expression(TObject.CreateString(interval_form));
      f =  Parser.Util.ResolveFunctionName("intervalob", exp_list);
      exp.AddElement(f);
      exp.Text.Append("INTERVAL").Append(" ").Append(t.image).Append(" ").Append(interval_form);
    }
  )

// Exists function
| (
    <EXISTS> "(" select_expression = GetTableSelectExpression() ")" {
      exp_list = new Expression[1];
      exp_list[0] = new Expression(select_expression);
      f = Parser.Util.ResolveFunctionName("sql_exists", exp_list);
      exp.AddElement(f);
      exp.Text.Append("EXISTS(").Append(select_expression.ToString()).Append(")");
    }
  )
  

// Unique function
| (
    <UNIQUE> "(" select_expression = GetTableSelectExpression() ")" {
      exp_list = new Expression[1];
      exp_list[0] = new Expression(select_expression);
      f = Parser.Util.ResolveFunctionName("sql_unique", exp_list);
      exp.AddElement(f);
      exp.Text.Append("UNIQUE(").Append(select_expression.ToString()).Append(")");
    }
  )

// Query expression
| (
    select_expression = GetTableSelectExpression() {
        exp.AddElement(select_expression);
        exp.Text.Append(select_expression.ToString());
    }
  )

// object instantiation
    | ( <NEW> f = Instantiation()
          { exp.AddElement(f); exp.Text.Append(f); }
      )
          
    | (   t=<STRING_LITERAL>
        | t=<BOOLEAN_LITERAL>
        | t=<NULL_LITERAL>
      ) { param_ob = Parser.Util.ToParamObject(t, case_insensitive_identifiers); 
          exp.AddElement(param_ob);
          exp.Text.Append(t.image);
        }
        
    | ( [ <ADD> | <SUBTRACT> { negative = true; } ]
        (
            t=<NUMBER_LITERAL>
          | t=<QUOTED_VARIABLE>        // (eg. '"rel1"')
          | t=<DOT_DELIMINATED_REF>    // (eg. 're1.re2.id')
          | t=<QUOTED_DELIMINATED_REF> // (eg. '"re1"."re2"."id"')
          | t=SQLIdentifier()
        )
      ) { if (t.kind == SQLConstants.NUMBER_LITERAL) {
            param_ob = Parser.Util.ParseNumberToken(t, negative);
            exp.AddElement(param_ob);
          }
          else {
            param_ob = Parser.Util.ToParamObject(t, case_insensitive_identifiers); 
            if (negative) {
              exp.AddElement(Parser.Util.Zero);
              exp.AddElement(param_ob);
              exp.AddElement(Operator.Get("-"));
            }
            else {
              exp.AddElement(param_ob);
            }
          }
          if (negative) {
            exp.Text.Append('-');
          }
          exp.Text.Append(t.image);
          
        }
  )

}

void SubQueryExpression(Expression exp, Stack stack) :
{ TableSelectExpression select;
  Expression[] exp_arr;
}
{
  // Parse the subquery list (either a list or a select statement)
  "("
  (   select=GetTableSelectExpression()
      { exp.AddElement(select);
        exp.Text.Append(" [SELECT]"); }
    | exp_arr=ExpressionList()
      { exp.AddElement(Parser.Util.ToArrayParamObject(exp_arr));
        exp.Text.Append(" (" + Parser.Util.ExpressionListToString(exp_arr) + ")");
      }
  )
  ")"
}


// Parses a simple positive integer constant.
int PositiveIntegerConstant() :
{ Token t;
}
{
  t = <NUMBER_LITERAL>

  { int val = Int32.Parse(t.image);
    if (val < 0) throw GenerateParseException();
    return val;
  }
}


void SubQueryOperator(Expression exp, Stack stack) :
{ Token t;
  String op_string;
  String query_type = "SINGLE";
  Operator op;
}
{
  (   LOOKAHEAD(2) (   <IN> { op = Operator.Get("IN"); }
                     | <NOT> <IN> { op = Operator.Get("NOT IN"); }
      )
  
    | (  op_string = GetSubQueryBooleanOperator() { op = Operator.Get(op_string); }
    [ ( t=<ANY> | t=<ALL> | t=<SOME> ) { query_type=t.image; } ]
    { op = op.GetSubQueryForm(query_type);
    }
      )
  )

  { expOperator(exp, stack, op);
    exp.Text.Append(" " + op + " ");
  }          
  
}

void BetweenPredicate(Expression exp, Stack stack) :
{ bool not_s = false;
  Expression exp1, exp2;
}
{   [ <NOT> { not_s = true; } ] <BETWEEN>
           exp1=DoNonBooleanExpression() <AND> exp2=DoNonBooleanExpression()

  { // Flush the operator stack to precedence 8
    flushOperatorStack(exp, stack, 8);
    // Get the end expression
    Expression end_exp = exp.EndExpression;
    if (not_s) {
      exp.Concat(exp1);
      exp.AddElement(Operator.Get("<"));
      exp.Concat(end_exp);
      exp.Concat(exp2);
      exp.AddElement(Operator.Get(">"));
      exp.AddElement(Operator.Get("or"));
      exp.Text.Append(" not between ");
    }
    else {
      exp.Concat(exp1);
      exp.AddElement(Operator.Get(">="));
      exp.Concat(end_exp);
      exp.Concat(exp2);
      exp.AddElement(Operator.Get("<="));
      exp.AddElement(Operator.Get("and"));
      exp.Text.Append(" between ");
    }
    exp.Text.Append(exp1.Text.ToString());
    exp.Text.Append(" and ");
    exp.Text.Append(exp2.Text.ToString());

  }

}

void BooleanOperator(Expression exp, Stack stack) :
{ Token t;
  String op_string;
  Operator op;
}
{
  (   op_string = GetBooleanOperator() { op = Operator.Get(op_string); }
  )

  { expOperator(exp, stack, op);
    exp.Text.Append(" " + op + " ");
  }          
}

void NumericOperator(Expression exp, Stack stack) :
{ Token t;
  String op_string;
  Operator op;
}
{
  (   op_string = GetNumericOperator() { op = Operator.Get(op_string); }
  )

  { expOperator(exp, stack, op);
    exp.Text.Append(" " + op + " ");
  }          
}

void StringOperator(Expression exp, Stack stack) :
{ Token t;
  String op_string;
  Operator op;
}
{
  (   op_string = GetStringOperator() { op = Operator.Get(op_string); }
  )

  { expOperator(exp, stack, op);
    exp.Text.Append(" " + op + " ");
  }          
}

String GetBooleanOperator() :
{ Token t;
}
{
  (   t=<ASSIGNMENT> | t=<EQUALS> | t=<GR> | t=<LE> | t=<GREQ> | t=<LEEQ>
    | t=<NOTEQ>
    | LOOKAHEAD(2) <IS> <NOT> { return "IS NOT"; } | t=<IS> 
    | t=<LIKE> | <NOT> <LIKE> { return "NOT LIKE"; }
    | LOOKAHEAD(2) <SOUNDS> <LIKE> { return "SOUNDS LIKE"; }
    | t=<AND> | t=<OR>
  )
  { return t.image; }
}

String GetSubQueryBooleanOperator() :
{ Token t;
}
{
  (   t=<ASSIGNMENT> | t=<EQUALS> | t=<GR> | t=<LE> | t=<GREQ> | t=<LEEQ> | t=<NOTEQ>
  )
  { return t.image; }
}

String GetNumericOperator() :
{ Token t;
}
{
   (   t=<DIVIDE> | t=<ADD> | t=<SUBTRACT>
     | t=<STAR> | t = <MODULUS>  // This is "*" (multiply) 
   )
   { return t.image; }
}

String GetStringOperator() :
{ Token t;
}
{
   ( t=<CONCAT> )
   { return t.image; }
}



Token FunctionIdentifier() :
{ Token t;
}
{
  ( t = <IF> | t = <USER> | t = <IDENTITY> | t = <IDENTIFIER> )
  { return t; }
}

RoutineInvoke Function() :
{ Token t, t2 = null, t3 = null;
  RoutineInvoke f;
  Expression exp1, exp2;
  Expression[] exp_list;
  TType cast_type;
}
{
  ( // COUNT function requires special handling,
      ( t = <COUNT> "(" [ <DISTINCT> { t.image="distinct_count"; } ] exp_list = FunctionParams() ")" )
    // TRIM function  
    | ( t = <TRIM> "(" [ LOOKAHEAD(3) [ t2=<LEADING> | t2=<BOTH> | t2=<TRAILING> ]
                         [ t3=<STRING_LITERAL> ] <FROM> ] exp1=DoExpression() ")" ) 
                        { exp_list = new Expression[3];
                          String ttype = t2 == null ? "both" : t2.image.ToLower();
                          Object str_char = t3 == null ? TObject.CreateString(" ") :
                                                         Parser.Util.ToParamObject(t3, case_insensitive_identifiers);
                          exp_list[0] = new Expression(TObject.CreateString(ttype));
                          exp_list[0].Text.Append("'" + ttype + "'");
                          exp_list[1] = new Expression(str_char);
                          exp_list[1].Text.Append("'" + str_char + "'");
                          exp_list[2] = exp1;
                          return Parser.Util.ResolveFunctionName("sql_trim", exp_list);
                        }

    | (t = <EXTRACT> "(" ( t2 = <YEAR> | t2=<MONTH> | t2=<DAY> | t2=<HOUR> | t2=<MINUTE> | t2=<SECOND> )
                           <FROM> exp1=DoExpression() ")" )
                        { exp_list = new Expression[2];
                          exp_list[0] = new Expression(TObject.CreateString(t2.image.ToLower()));
                          exp_list[0].Text.Append("'" + t2.image + "'");
                          exp_list[1] = exp1;
                          return Parser.Util.ResolveFunctionName("extract", exp_list);
                        }
    // CAST function
    | ( t = <CAST> "(" exp1=DoExpression() <AS> cast_type=GetTType() ")" )
                        { exp_list = new Expression[2];
                          String enc_form = TType.Encode(cast_type);
                          exp_list[0] = exp1;
                          exp_list[1] = new Expression(TObject.CreateString(enc_form));
                          exp_list[1].Text.Append("'" + enc_form + "'");
                          return Parser.Util.ResolveFunctionName("sql_cast", exp_list);
                        }
    // Parse a function identifier and function parameter list.
    | ( t = FunctionIdentifier() "(" exp_list = FunctionParams() ")" )
//    // IF function
//    | ( t = <IF> "(" exp_list = FunctionParams() ")" )
//    // Standard functions.
//    | ( t = <IDENTIFIER> "(" exp_list = FunctionParams() ")" )
  )

  { return Parser.Util.ResolveFunctionName(t.image, exp_list); }  
}

// An instantiation of an object.  For example, 'System.Drawing.Point(40, 30)'
RoutineInvoke Instantiation() :
{ Token t;
  Expression[] args;
}
{
  // PENDING: Handling arrays (eg. 'System.String[] { 'Antonello', 'Provenzano' }' or 'double[] { 25, 2, 75, 26 }' )
  t=<DOT_DELIMINATED_REF> "(" args=ExpressionList() ")"
  
  { Expression[] comp_args = new Expression[args.Length + 1];
    Array.Copy(args, 0, comp_args, 1, args.Length);
    comp_args[0] = new Expression(TObject.CreateString(t.image));
    comp_args[0].Text.Append("'" + t.image + "'");
    return Parser.Util.ResolveFunctionName("_new_Object", comp_args); }
}

// Parameters for a function
Expression[] FunctionParams() :
{ Expression[] exp_list;
}
{
  ( <STAR> { exp_list = FunctionFactory.GlobList; }
    | exp_list = ExpressionList()
  )
  
  { return exp_list; }
}



Expression[] ExpressionList() :
{ ArrayList list = new ArrayList();
  Expression e;
}
{
  [ e = DoExpression() { list.Add(e); }
    ( "," e = DoExpression() { list.Add(e); }  )*
  ]
  
  { return (Expression[]) list.ToArray(typeof(Expression)); }
}


CompositeFunction GetComposite() :
{ CompositeFunction composite = CompositeFunction.None; }
{
  ( 
     <UNION> { composite = CompositeFunction.Union; } | 
     <INTERSECT> { composite = CompositeFunction.Intersect; } | 
     <EXCEPT> { composite = CompositeFunction.Except; }
  )
  
  { return composite; }
}



String TableName() :
{ Token name;
}
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <OLD> | name = <NEW> |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }

}

String SequenceName() :
{ Token name;
}
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

String TriggerName() :
{ Token name;
}
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

String IndexName() :
{ Token name;
}
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// A username
String UserName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = <IDENTIFIER> | name = <PUBLIC> )
  
  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Name of a schema
String SchemaName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )
  
  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Name of a constraint name
String ConstraintName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )
  
  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Parses a column name  
String ColumnName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Parses a column name as a VariableName object  
VariableName ColumnNameVariable() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return (VariableName) Parser.Util.ToParamObject(name, case_insensitive_identifiers); } 
}

// Parses an aliased table name  
String TableAliasName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Parses a procedure name  
String ProcedureName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Parses a function name
String FunctionName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Parses the name of an argument in a procedure/function declaration
String ProcArgumentName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )
  
  { return CaseCheck(Parser.Util.AsNonQuotedRef(name)); }
}

// Parses an SQL identifier
Token SQLIdentifier() :
{ Token name; }
{
  (   name = <IDENTIFIER>
    | name = <OPTION> | name = <ACCOUNT> | name = <PASSWORD>
    | name = <PRIVILEGES> | name = <GROUPS> | name = <LANGUAGE>
    | name = <NAME> | name = <CSHARP> | name = <ACTION>
    | name = <YEAR> | name = <MONTH> | name = <DAY> | name = <HOUR>
    | name = <MINUTE> | name = <SECOND>
  )
  
  { return name; }
}