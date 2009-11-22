options {
  STATIC = false;
  UNICODE_INPUT = true;
  OPTIMIZE_TOKEN_MANAGER = true;
//  IGNORE_CASE = true;
  DEBUG_PARSER = false;
}

PARSER_BEGIN(SelectParser)

namespace Deveel.Data.Select;

using System;
using System.Collections;
using System.Text;

internal class SelectParser {  
	private ArrayList parameters;
	private int parameter_id = 0;

	public IList Parameters {
		get { return parameters; }
	}

	public void Reset() {
		parameter_id = 0;
		parameters.Clear();
	}

	public void AddParameter(string image) {
		SelectParameter param = new SelectParameter(parameter_id, image);
		++parameter_id;
		parameters.Add(param);
	}

}

PARSER_END(SelectParser)


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
| <SET:         "set">
| <USE:         "use">
| <ASC:         "asc">
| <OLD:         "old">
| <NEW:         "new">
| <SQLADD:      "add">
| <FOR:         "for">
| <ROW:         "row">
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
| <USER:        "user">
| <CAST:        "cast">
| <LONG:        "long">
| <NAME:        "name">
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
| <CROSS:       "cross">
| <OUTER:       "outer">
| <CHECK:       "check">
| <USING:       "using">
| <UNION:       "union">
| <GRANT:       "grant">
| <USAGE:       "usage">
| <SQLRETURN:   "return">
| <BEFORE:      "before">
| <UNLOCK:      "unlock">
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
| <SEQUENCE:    "sequence">
| <RESTRICT:    "restrict">
| <PASSWORD:    "password">
| <TRAILING:    "trailing">
//| <NOTNULL:     "not null">
| <GROUPBY:     "group by">
| <ORDERBY:     "order by">
| <DEFERRED:    "deferred">
| <IDENTITY:    "identity">
| <DISTINCT:    "distinct">
| <LANGUAGE:    "language">
| <INCREMENT:   "increment">
| <PROCEDURE:   "procedure">
| <CHARACTER:   "character">
| <IMMEDIATE:   "immediate">
| <INITIALLY:   "initially">
| <TEMPORARY:   "temporary">
| <INTERSECT:   "intersect">
| <PRIVILEGES:  "privileges">
| <CONSTRAINT:  "constraint">
| <DEFERRABLE:  "deferrable">
| <REFERENCES:  "references">

| <PRIMARY:     "primary">
| <FOREIGN:     "foreign">
| <KEY:         "key">

| <INDEX_NONE:  "index_none">
| <INDEX_BLIST: "index_blist">

| <GROUPMAX:    "group max">

| <COLLATE:     "collate">

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

// Collate strengths,

| <PRIMARY_STRENGTH:   "primary_strength">
| <SECONDARY_STRENGTH: "secondary_strength">
| <TERTIARY_STRENGTH:  "tertiary_strength">
| <IDENTICAL_STRENGTH: "identical_strength">

// Collate decomposition levels,

| <NO_DECOMPOSITION:        "no_decomposition">
| <CANONICAL_DECOMPOSITION: "canonical_decomposition">
| <FULL_DECOMPOSITION:      "full_decomposition">


// Current date/time/timestamp literals

| <CURRENT_TIME:      "current_time">
| <CURRENT_DATE:      "current_date">
| <CURRENT_TIMESTAMP: "current_timestamp">

// Current timezone

| <DBTIMEZONE:        "dbtimezone">


| <LIKE:       "like" >
| <REGEX:      "regex" >
| <AND:        "and" >
| <OR:         "or" >
| <IN:         "in" >
| <NOT:        "not">

}

TOKEN : {

    <NUMBER_LITERAL:
       ( ( (["0"-"9"])+ ( "." (["0"-"9"])+ )? )
    | ( "." (["0"-"9"])+ ) )
               ( "E" (["-","+"])? (["0"-"9"])+ )? 
    >
  | <STRING_LITERAL:   "'" ( "''" | "\\" ["a"-"z", "\\", "%", "_", "'"] | ~["'","\\"] )* "'" >
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

| <#LETTER: ["a"-"z", "A"-"Z", "_"] >
| <#DIGIT: ["0"-"9"]>

}

string ParseExpression() :
{ string exp;
}
{
  exp = DoExpression() <EOF>
  
  { return exp; }
}

// Statement that ends with a ';'
SelectExpression ParseSelect() :
{ SelectExpression exp; }
{
  (
    ( exp=SelectExpression() )
    ( ";" | <EOF> )
  )

  { return exp; }
}

SelectExpression SelectExpression() :
{ SelectExpression exp; }
{
  ( exp = GetTableSelectExpression()
    [ <ORDERBY> SelectOrderByList(exp.OrderBy) ]
  )

  { return exp; }
}


// A table expression 
SelectExpression GetTableSelectExpression() :
{ SelectExpression table_expr = new SelectExpression();
  CompositeFunction composite = CompositeFunction.None;
  bool is_all = false;
  SelectExpression next_composite_expression;
  string whereClause, havingClause;
}
{
  ( <SELECT>
        ( 
          LOOKAHEAD(2) <IDENTITY> { table_expr.Columns.Add(Sql.SelectColumn.Identity); } |
          [ table_expr.Distinct = SetQuantifier() ] 
          SelectColumnList(table_expr.Columns) 
        )
        [ <FROM> SelectTableList(table_expr.From) ]
        [ <WHERE> whereClause=DoExpression() { table_expr.Where = whereClause; } ]

        [ <GROUPBY> SelectGroupByList(table_expr.GroupBy)
          [ <GROUPMAX> table_expr.GroupMax = GroupMaxColumn() ] 
          [ <HAVING> havingClause = DoExpression() { table_expr.Having = havingClause; } ] ]

        [ composite = GetComposite() [ <ALL> { is_all = true; } ]
          next_composite_expression = GetTableSelectExpression()
          { table_expr.ChainComposite(next_composite_expression, composite, is_all); } 
        ]
  )
  { return table_expr; }
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
{ SelectColumn col = new SelectColumn();
  String aliased_name;
  Token t;
  string exp;
}
{ 
  (   exp = DoExpression() { col.SetExpression(exp); } [ <AS> ] [ aliased_name=TableAliasName() { col.SetAlias(aliased_name); } ]
    | <STAR> { col = Select.SelectColumn.Glob("*"); }
    | t = <GLOBVARIABLE> { col = Select.SelectColumn.Glob(t.image); }
    | t = <QUOTEDGLOBVARIABLE> { col = Select.SelectColumn.Glob(t.image); }
  )
  { return col; }
}

void SelectGroupByList(IList list) :
{ ByColumn col;
  string exp;
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
string GroupMaxColumn() :
{ string columnName; }
{
  columnName = ColumnName()
  { return columnName; }
}



void SelectOrderByList(IList list) :
{ string exp;
  bool ascending = true;
}
{
    exp = DoExpression() [ ascending=OrderingSpec() ]
                         { list.Add(new ByColumn(exp, ascending)); }
  ( "," exp = DoExpression() { ascending=true; } [ ascending=OrderingSpec() ]
                         { list.Add(new ByColumn(exp, ascending)); } )*
  
}

bool OrderingSpec() :
{}
{
  ( <ASC> { return true; } | <DESC> { return false; } )
  
  { return true; }
}


void TableDeclaration(FromClause from_clause) :
{ String table=null, declare_as = null;
  SelectExpression select_stmt = null;
}
{

  ( ( table=TableName() | "(" select_stmt=GetTableSelectExpression() ")" )
    [ [ <AS> ] declare_as=TableName() ] )
  
  { from_clause.AddTable(table, select_stmt, declare_as); }

}

void SelectTableList(FromClause from_clause) :
{}
{
  TableDeclaration(from_clause) [ FromClauseJoin(from_clause) ]
}

void FromClauseJoin(FromClause from_clause) :
{ string on_expression; }
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
        { from_clause.AddPreviousJoin(JoinType.LeftOuter, on_expression); }
      ) [ FromClauseJoin(from_clause) ]
    | (
        <RIGHT> [<OUTER>] <JOIN> TableDeclaration(from_clause) <ON> on_expression=DoExpression()
        { from_clause.AddPreviousJoin(JoinType.RightOuter, on_expression); }
      ) [ FromClauseJoin(from_clause) ]
  )

}

string DoExpression() :
{ ExpressionBuilder sb = new ExpressionBuilder(); }
{
  expression(sb)

  { return sb.ToString(); }
}

string DoNonBooleanExpression() :
{ ExpressionBuilder sb = new ExpressionBuilder(); }
{

  nonBooleanExpression(sb)

  { return sb.ToString(); }
}


/**
 * Parse an expression.
 */
void expression(ExpressionBuilder sb) :
{ }
{
  Operand(sb) ( LOOKAHEAD(2) OpPart(sb) )*

}

/**
 * Parses a non-bool expression.
 */
void nonBooleanExpression(ExpressionBuilder sb) :
{
}
{
  
  Operand(sb)
     ( LOOKAHEAD(2) (  StringOperator(sb)
                     | NumericOperator(sb) ) Operand(sb) )* 

}

void OpPart(ExpressionBuilder sb) :
{ Token t;
  string regex_expression;
}
{

  (   LOOKAHEAD(3) (   BooleanOperator(sb)
                     | NumericOperator(sb)
                     | StringOperator(sb) )
                   Operand(sb)

      // NOTE: Handling regex literals is a horrible horrible hack.  The <REGEX_LITERAL> 
      //   token starts with 'regex /' and the regex string follows.
    | (   t=<REGEX> { sb.Append(t.image); }
                  expression(sb)
                | t=<REGEX_LITERAL>
                  { sb.Append(t.image); }
       )

    | LOOKAHEAD(2) SubQueryOperator(sb) SubQueryExpression(sb) 

    | BetweenPredicate(sb) 

  )
  
}  
  

void Operand(ExpressionBuilder sb) :
{ Token t, tt;
  string f;
  Expression[] exp_list;
  String time_fname;
  bool negative = false;
  Object param_ob;
  Object param_resolve;
}
{
  (   "(" { sb.Append("("); }
        expression(sb) 
       ")" { sb.Append(")"); }
    | t = <PARAMETER_REF> 
          { AddParameter(t.image); sb.Append(t.image);  }
    | t = <NAMED_PARAMETER>
          { AddParameter(t.image); sb.Append(t.image); }

    | LOOKAHEAD(2) <NOT>
      { sb.Append("NOT"); }
      Operand(sb)
          
    | LOOKAHEAD(3) ( f = Function() { sb.Append(f); } ) 

// Time values
| ( (   tt=<DATE> { sb.Append("DATE"); }
      | tt=<TIME> { sb.Append("TIME"); } 
      | tt=<TIMESTAMP> { sb.Append("TIMESTAMP"); }
    )
    t=<STRING_LITERAL>
    { sb.Append(t.image); }
  )

// Current timestamp
| ( (   tt=<CURRENT_TIMESTAMP> { sb.Append("CURRENT_TIMESTAMP"); }
      | tt=<CURRENT_TIME>      { sb.Append("CURRENT_TIME"); }
      | tt=<CURRENT_DATE>      { sb.Append("CURRENT_DATE"); }
    )
  )

// Current timezone
| (
     tt=<DBTIMEZONE> { sb.Append("DBTIMEZONE"); }
  )
      
// object instantiation
    | ( <NEW> f = Instantiation()
          { sb.Append(f); }
      )
          
    | (   t=<STRING_LITERAL>
        | t=<BOOLEAN_LITERAL>
        | t=<NULL_LITERAL>
      ) { sb.Append(t.image); }
        
    | ( [ <ADD> { sb.Append("+"); } | <SUBTRACT> { sb.Append("-"); } ]
        (
            t=<NUMBER_LITERAL>
          | t=<QUOTED_VARIABLE>        // (eg. '"rel1"')
          | t=<DOT_DELIMINATED_REF>    // (eg. 're1.re2.id')
          | t=<QUOTED_DELIMINATED_REF> // (eg. '"re1"."re2"."id"')
          | t=SQLIdentifier()
        )
      ) { sb.Append(t.image); }
  )

}

void SubQueryExpression(ExpressionBuilder sb) :
{ SelectExpression select; }
{
  // Parse the subquery list (either a list or a select statement)
  "(" { sb.Append("("); }
    ( select=GetTableSelectExpression() { sb.Append(select.ToString()); }
    | ExpressionList(sb) )
  ")" { sb.Append(")"); }
}

void SubQueryOperator(ExpressionBuilder sb) :
{ Token t;
  string op_string;
}
{
  (
    LOOKAHEAD(2) 
    ( t=<IN> { sb.Append(t.image); }
    | t=<NOT> { sb.Append(t.image); } t=<IN> { sb.Append(t.image); } )
  
  | ( op_string = GetSubQueryBooleanOperator() { sb.Append(op_string); }
    [ ( t=<ANY> | t=<ALL> | t=<SOME> ) { sb.Append(t.image); } ] ) )

  {  }
}

void BetweenPredicate(ExpressionBuilder sb) :
{ Token t; 
  string exp;
}
{   [ <NOT> { sb.Append("NOT"); } ] 
    <BETWEEN> { sb.Append("BETWEEN"); }
    exp = DoNonBooleanExpression() { sb.Append(exp); }
    <AND> { sb.Append("AND"); } 
    exp = DoNonBooleanExpression() { sb.Append(exp); }
  { }
}

void BooleanOperator(ExpressionBuilder exp) :
{ Token t;
  string op_string;
}
{
  ( op_string = GetBooleanOperator() { exp.Append(op_string); } )

  { }
}

void NumericOperator(ExpressionBuilder exp) :
{ Token t;
  string op_string;
}
{
  ( op_string = GetNumericOperator() { exp.Append(op_string); } )

  { }
}

void StringOperator(ExpressionBuilder exp) :
{ Token t;
  string op_string;
}
{
  ( op_string = GetStringOperator() { exp.Append(op_string); } )

  { }
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

string Function() :
{ 
   ExpressionBuilder sb = new ExpressionBuilder();
   string cast_type, exp;
   Token t, t2, t3;
}
{
  ( // COUNT function requires special handling,
      ( <COUNT> { sb.Append("COUNT"); } 
        "(" { sb.Append("("); } 
        [ <DISTINCT> { sb.Append("DISTINCT"); } ] 
        FunctionParams(sb) 
        ")" { sb.Append(")"); } )
    // TRIM function  
    | ( <TRIM> { sb.Append("TRIM"); } 
        "(" { sb.Append("("); } 
        [ LOOKAHEAD(3) [ t2=<LEADING> | t2=<BOTH> | t2=<TRAILING> { sb.Append(t2.image); } ]
          [ t3=<STRING_LITERAL> { sb.Append(t3.image); } ] 
          <FROM> { sb.Append("FROM"); }
        ] exp = DoExpression() { sb.Append(exp); }
        ")" { sb.Append(")"); } 
      ) 
    // CAST function
    | ( <CAST> { sb.Append("CAST"); } 
        "(" { sb.Append("("); } 
        exp = DoExpression() { sb.Append(exp); }
        <AS> { sb.Append("AS"); }
        cast_type=GetTType() { sb.Append(cast_type); } 
        ")" { sb.Append(")"); } )
    | ( t = FunctionIdentifier() { sb.Append(t.image); } 
        "(" { sb.Append("("); } 
        FunctionParams(sb) 
        ")" { sb.Append(")"); } )
  )

  { return sb.ToString(); }  
}

string GetStringSQLType() :
{ }
{
    LOOKAHEAD(2) ( <CHARACTER> <VARYING> ) { return "CHARACTER VARYING"; }
  | LOOKAHEAD(3) ( <LONG> <CHARACTER> <VARYING> ) { return "LONG CHARACTER VARYING"; }
  | ( <TEXT> | <STRING> | <LONGVARCHAR> ) { return "STRING"; }
  | ( <CHAR> | <CHARACTER> ) { return "CHAR"; }
  | <VARCHAR> { return "VARCHAR"; }
  | <CLOB> { return "CLOB"; }
}

string GetNumericSQLType() :
{ }
{
    ( <INT> | <INTEGER> ) { return "INT"; }
  | <TINYINT> { return "TINYINT"; }
  | <SMALLINT> { return "SMALLINT"; }
  | <BIGINT> { return "BIGINT"; }
  | <FLOAT> { return "FLOAT"; }
  | <REAL> { return "REAL"; }
  | <DOUBLE> { return "DOUBLE"; }
  | <NUMERIC> { return "NUMERIC"; }
  | <DECIMAL> { return "DECIMAL"; }
}

string GetBooleanSQLType() :
{ }
{
  ( <BOOLEAN> | <BIT> ) { return "BOOLEAN"; }
}

string GetDateSQLType() :
{ }
{
    <TIMESTAMP> { return "TIMESTAMP"; }
  | <TIME> { return "TIME"; }
  | <DATE> { return "DATE"; }
}

string GetBinarySQLType() :
{ }
{
    LOOKAHEAD(2) ( <BINARY> <VARYING> ) { return "BINARY VARYING"; }
  | LOOKAHEAD(3) ( <LONG> <BINARY> <VARYING> ) { return "LONG BINARY VARYING"; }
  | <LONGVARBINARY> { return "LONGVARBINARY"; }
  | <VARBINARY> { return "VARBINARY"; }
  | <BINARY> { return "BINARY"; }
  | <BLOB> { return "BLOB"; }
}

string GetIntervalSQLType() :
{ }
{
    <SECOND> { return "SECOND"; }
  | <MINUTE> { return "MINUTE"; }
  | <HOUR>   { return "HOUR"; }
  | <DAY>    { return "DAY"; }
  | <MONTH>  { return "MONTH"; }
  | <YEAR>   { return "YEAR"; }
  | <INTERVAL> { return "INTERVAL"; }
}

// Parses a simple positive integer constant.
string PositiveIntegerConstant() :
{ Token t;
}
{
  t = <NUMBER_LITERAL>

  { int val = Int32.Parse(t.image);
    if (val < 0) throw GenerateParseException();
    return val.ToString();
  }
}

string GetCollateStrength() :
{ 
}
{ (   <PRIMARY_STRENGTH>    { return "PRIMARY_STRENGTH"; }
    | <SECONDARY_STRENGTH>  { return "SECONDARY_STRENGTH"; }
    | <TERTIARY_STRENGTH>   { return "TERTIARY_STRENGTH"; }
    | <IDENTICAL_STRENGTH>  { return "IDENTICAL_STRENGTH"; }
  )
}

string GetCollateDecomposition() :
{ }
{ (   <NO_DECOMPOSITION>        { return "NO_DECOMPOSITION"; }
    | <CANONICAL_DECOMPOSITION> { return "CANONICAL_DECOMPOSITION"; }
    | <FULL_DECOMPOSITION>      { return "FULL_DECOMPOSITION"; }
  )
}

// Parses an SQL type and forms a TType object.  For example, "CHAR(500)" is
// parsed to a TStringType with a maximum size of 500 and lexicographical
// collation.
string GetTType() :
{ Token t;
  string data_type;
  string size = null;
  string scale = null;
  Token class_tok = null;
  string strength = null;
  string decomposition = null;
  ExpressionBuilder sb = new ExpressionBuilder();
}
{
  ( t=<OBJECT> { sb.Append(t.image); } 
    [ "(" { sb.Append("("); } 
      ( class_tok=<DOT_DELIMINATED_REF> | class_tok=<OBJECT_ARRAY_REF> ) { sb.Append(class_tok.image); }
      ")" { sb.Append(")"); } ]
      { return sb.ToString(); } 

    | LOOKAHEAD(GetStringSQLType())
      data_type = GetStringSQLType() { sb.Append(data_type); } 
      [ "(" { sb.Append("("); } 
        size = PositiveIntegerConstant() { sb.Append(size); } 
        ")" { sb.Append(")"); } ]
      [ t=<COLLATE> { sb.Append(t.image); } 
        t=<STRING_LITERAL> { sb.Append(t.image); }
        [ strength=GetCollateStrength() { sb.Append(strength); } ] 
        [ decomposition = GetCollateDecomposition() { sb.Append(decomposition); } ] 
      ]
      { return sb.ToString(); }

    | data_type = GetNumericSQLType() { sb.Append(data_type); } 
      [ "(" { sb.Append("("); } 
        size = PositiveIntegerConstant() { sb.Append(size); }
          [ "," { sb.Append(","); } scale = PositiveIntegerConstant() { sb.Append(scale); } ] 
        ")" { sb.Append(")"); } ]
      { return sb.ToString(); } 
      
    | data_type = GetBooleanSQLType() { sb.Append(data_type); }
      { return sb.ToString(); }

    | data_type = GetDateSQLType() { sb.Append(data_type); }
      { return sb.ToString(); }

    | data_type = GetBinarySQLType() { sb.Append(data_type); }
      [ "(" { sb.Append("("); } 
        size = PositiveIntegerConstant() { sb.Append(size); } 
        ")" { sb.Append(")"); } ]
      { return sb.ToString(); }

    | data_type = GetIntervalSQLType() { sb.Append(data_type); } 
      [ "(" { sb.Append("("); } 
        size = PositiveIntegerConstant() { sb.Append(size); } 
        ")" { sb.Append(")"); } ]
      { return sb.ToString(); }
  )
}

// An instantiation of an object.  For example, 'System.Drawing.Point(40, 30)'
string Instantiation() :
{ Token t;
  ExpressionBuilder sb = new ExpressionBuilder();
}
{
  // PENDING: Handling arrays (eg. 'System.String[] { 'Antonello', 'Provenzano' }' or 'double[] { 25, 2, 75, 26 }' )
  t=<DOT_DELIMINATED_REF> { sb.Append(t.image); } 
  "(" { sb.Append("("); } 
  ExpressionList(sb) 
  ")"
  
  { return sb.ToString(); }
}

// Parameters for a function
void FunctionParams(ExpressionBuilder sb) :
{ }
{
  ( <STAR> { sb.Append("*"); }
    | ExpressionList(sb)
  )
  
  { }
}



void ExpressionList(ExpressionBuilder sb) :
{ string exp; }
{
  [ exp = DoExpression() { sb.Append(exp); }
    ( "," { sb.Append(", "); } exp = DoExpression() { sb.Append(exp); }  )*
  ]
  
  { }
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

  { return name.image; }

}

// Parses a column name  
String ColumnName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return name.image; }
}

// Parses an aliased table name  
string TableAliasName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )

  { return name.image; }
}

// Parses a function name
string FunctionName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() |
    name = <DOT_DELIMINATED_REF> | name = <QUOTED_DELIMINATED_REF> )

  { return name.image; }
}

// Parses the name of an argument in a procedure/function declaration
String ProcArgumentName() :
{ Token name; }
{
  ( name = <QUOTED_VARIABLE> | name = SQLIdentifier() )
  
  { return name.image; }
}

// Parses an SQL identifier
Token SQLIdentifier() :
{ Token name; }
{
  (   name = <IDENTIFIER>
    | name = <OPTION> | name = <ACCOUNT> | name = <PASSWORD>
    | name = <PRIVILEGES> | name = <GROUPS> | name = <LANGUAGE>
    | name = <NAME> | name = <CSHARP> | name = <ACTION>
  )
  
  { return name; }
}