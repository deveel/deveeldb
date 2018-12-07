parser grammar PlSqlParser;

options {
	tokenVocab=PlSqlLexer;
}

// $<Expression & Condition
expressionUnit
    : expression EOF
	;

expression_list
    : '(' expression (',' expression)* ')'
    ;

condition
    : expression
    ;

conditionWrapper
    : expression
    ;

expression
    : logicalAndExpression ( OR logicalAndExpression )*
    ;

expressionWrapper
    : expression
    ;

logicalAndExpression
    : negatedExpression ( AND negatedExpression )*
    ;

negatedExpression
    : NOT negatedExpression
    | equalityExpression
    ;

equalityExpression
    : relationalExpression (IS NOT? ( NULL | NAN | EMPTY | UNKNOWN | OF TYPE? '(' datatype ')') )*
    ;

relationalExpression
    : compoundExpression
      (relationalOperator compoundExpression)*
    ;

relationalOperator
    : ( op='=' | notEqual | op='<' | op='>' | lessThanOrEquals | greaterThanOrEquals )
	;

compoundExpression
    : exp=concatenation
      ( NOT? ( IN inElements | 
	           BETWEEN min=concatenation AND max=concatenation | 
			   LIKE likeExp=concatenation likeEscapePart?) )?
    ;

likeEscapePart
    : ESCAPE concatenation
    ;


inElements
    : '(' subquery ')' #InSubquery
    | '(' concatenationWrapper (',' concatenationWrapper)* ')' #InArray
    | bind_variable #InVariable
    | general_element #InElement
    ;

concatenation
    : additiveExpression (concat additiveExpression)*
    ;

concatenationWrapper
    : concatenation
    ;

additiveExpression
    : multiplyExpression (additiveOperator multiplyExpression)*
    ;

additiveOperator
    : ( '+' | '-' )
	;

multiplyExpression
    : unaryExpression (multiplyOperator unaryExpression)*
    ;

multiplyOperator
    : ( '*' | '/' | '%' )
	;

unaryExpression
    : unaryplusExpression
    | unaryminusExpression
    | caseExpression
    | quantifiedExpression
    | standardFunction
    | atom
    ;

unaryplusExpression
    : '+' unaryExpression
	;

unaryminusExpression
    : '-' unaryExpression
	;

// $<CASE - Specific Clauses

caseExpression
   : searchedCaseExpression
   | simpleCaseExpression
   ;

searchedCaseExpression
   : CASE simpleCaseWhenExpressionPart+ caseElseExpressionPart? END CASE?
   ;

simpleCaseWhenExpressionPart
   : WHEN conditionWrapper THEN expressionWrapper
   ;

caseElseExpressionPart
   : ELSE expressionWrapper
   ;

simpleCaseExpression
   : CASE atom simpleCaseWhenExpressionPart+ caseElseExpressionPart? END CASE?
   ;

// $>

// $<Select - Specific Clauses

subquery
    : subqueryBasicElements subquery_operation_part*
    ;

subquery_operation_part
    : (UNION ALL? | INTERSECT | ( MINUS | EXCEPT) ) subqueryBasicElements
    ;

subqueryBasicElements
    : queryBlock
    | '(' subquery ')'
    ;

queryBlock
    : SELECT (DISTINCT | UNIQUE | ALL)? (all='*' | selectedElement (',' selectedElement)*)
      into_clause? fromClause? whereClause? (groupByClause | groupMaxClause)? 
    ;

selectedElement
    : (expression | selectedColumn ) column_alias?
    ;

fromClause
    : FROM tableRefList
    ;

selectedColumn
    : objectName ('.' glob= '*')?
	;

tableRefList
    : tableRef (',' tableRef)*
    ;

tableRef
    : queryExpressionClause joinClause*
    ;


joinClause
    : (INNER | outerJoinType)? 
      JOIN queryExpressionClause joinOnPart
    ;

joinOnPart
    : ON condition
    ;

outerJoinType
    : (FULL | LEFT | RIGHT) OUTER?
    ;

groupByClause
    : GROUP BY groupByElements havingClause?
    | havingClause (GROUP BY groupByElements )?
    ;

groupMaxClause
    : GROUP MAX objectName
	;

groupByElements
    : expression (',' expression )*
    ;

havingClause
    : HAVING condition
    ;

orderByClause
    : ORDER BY orderByElements
    ;

orderByElements
    : orderByElement (',' orderByElement)*
	;

orderByElement
    : expression (ASC | DESC)?
    ;

forUpdateClause
    : FOR UPDATE
    ;

queryExpressionClause
    : (( '(' subquery ')' | objectName ) (AS? alias=regular_id)?)
    ;

// $>

atom
    : bind_variable
    | constant
    | general_element
	| subquery
	| group
    ;

group
    : '(' expressionOrVector ')'
	;

expressionOrVector
    : expression (vectorExpression)?
    ;

vectorExpression
    : ',' expression (',' expression)*
    ;

quantifiedExpression
    : (SOME | ALL | ANY) ('(' subquery ')' | expression_list )
    ;

standardFunction
    : CURRENT_TIME #CurrentTimeFunction
	| CURRENT_TIMESTAMP #CurrentTimeStampFunction
	| CURRENT_DATE #CurrentDateFunction
	| NEXT VALUE FOR objectName #NextValueFunction
	| COUNT '(' (all='*' | ((DISTINCT | UNIQUE | ALL)? concatenationWrapper)) ')' #CountFunction
    | CAST '(' (MULTISET '(' subquery ')' | concatenationWrapper) AS datatype ')' #CastFunction
    | EXTRACT '(' regular_id FROM concatenationWrapper ')' #ExtractFunction
    | TRIM '(' ((LEADING | TRAILING | BOTH)? quoted_string? FROM)? concatenationWrapper ')' #TrimFunction
	| objectName '(' (argument (',' argument)*)? ')' #InvokedFunction
    ;

// Common

column_alias
    : AS? (id | alias_quoted_string)
    | AS
    ;

alias_quoted_string
    : quoted_string
    ;

whereClause
    : WHERE (current_of_clause | conditionWrapper)
    ;

current_of_clause
    : CURRENT OF cursor_name
    ;

into_clause
    : INTO ( objectName | variable_name (',' variable_name)* )
    ;

// $>

// $<Common PL/SQL Specs

function_argument
    : '(' argument? (',' argument )* ')'
    ;

argument
    : (id '=' '>')? expressionWrapper
    ;

bind_variable
    : (BINDVAR | ':' UNSIGNED_INTEGER)
    ;

general_element
    : objectName function_argument?
    ;

// $>

// $<Data Types

datatype
    : primitiveType #PrimitiveDataType
    | INTERVAL (top=YEAR | top=DAY) ('(' expressionWrapper ')')? TO (bottom=MONTH | bottom=SECOND) ('(' expressionWrapper ')')? #IntervalType
	| objectName typeArgument? #UserDataType
	| rowRefType #RowType
	| columnRefType #ColumnType
    ;

typeArgument
    : '(' typeArgumentSpec (',' typeArgumentSpec)* ')'
	;

typeArgumentSpec
    : ( id '=' )? (numeric | quoted_string )
	;

primitiveType
    : (integerType | numericType | booleanType | stringType | binaryType | timeType)
	;

integerType
    : (TINYINT | SMALLINT | BIGINT | INT | INTEGER) ('(' numeric ')')?
	;

numericType
    : (FLOAT | REAL | DOUBLE | NUMERIC | DECIMAL) ( '(' precision=numeric (',' scale=numeric)? ')' )?
	;

booleanType
    : (BOOLEAN | BIT)
	;

binaryType
    : (BLOB | BINARY | VARBINARY | longVarbinary) ( '(' (numeric | MAX) ')' )?
	;

stringType
    : (CLOB | VARCHAR | CHAR | longVarchar | STRING) ( '(' (numeric | MAX) ')' )? 
	     (LOCALE locale=CHAR_STRING)?
	;


longVarchar
    : LONG CHARACTER VARYING
	;

longVarbinary
    : LONG BINARY VARYING
	;

timeType
    : (DATE | TIMESTAMP | TIME | DATETIME ) (WITH local=LOCAL? TIME ZONE)?
	;

rowRefType
    : objectName PERCENT_ROWTYPE
	;

columnRefType
    : objectName PERCENT_TYPE
	;


// $>

// $<Common PL/SQL Named Elements

schema_name
    : id
    ;

parameter_name
    : id
    ;

variable_name
    : id
    | bind_variable
    ;

cursor_name
    : id
    | bind_variable
    ;

exception_name
    : id 
    ;

objectName
   : id ('.' id)*
   ;

columnName
   : id
   ;

labelName
   : id
   ;

// $>

// $<Lexer Mappings

constant
    : TIMESTAMP (argString=quoted_string | bind_variable) (AT TIME ZONE tzString=quoted_string)? #TimeStampFunction
    | numeric #ConstantNumeric
    | DATE quoted_string #DateImplicitConvert
    | quoted_string #ConstantString
    | NULL #ConstantNull
	| UNKNOWN #ConstantUnknown
    | TRUE #ConstantTrue
    | FALSE #ConstantFalse
    | DBTIMEZONE  #ConstantDBTimeZone
    | USERTIMEZONE #ConstantUserTimeZone
    | MINVALUE #ConstantMinValue
    | MAXVALUE #ConstantMaxValue
    ;

numeric
    : UNSIGNED_INTEGER
    | APPROXIMATE_NUM_LIT
    ;

quoted_string
    : CHAR_STRING
    //| CHAR_STRING_PERL
    | NATIONAL_CHAR_STRING_LIT
    ;

id
    : regular_id
    | DELIMITED_ID
    ;

notEqual
    : NOT_EQUAL_OP
    | '<' '>'
    | '!' '='
    | '^' '='
    ;

greaterThanOrEquals
    : '>='
    | '>' '='
    ;

lessThanOrEquals
    : '<='
    | '<' '='
    ;

concat
    : '||'
    | '|' '|'
    ;

regular_id
    : REGULAR_ID
    | A_LETTER
	| ABSOLUTE
	| ACTION
	| ACCOUNT
    | ADD
	| ADMIN
    | AFTER
    | AGGREGATE
    //| ALL
    //| ALTER
    //| AND
    //| ANY
    | ARRAY
    // | AS
    //| ASC
    | AT
    | ATTRIBUTE
    | AUTO
    | BEFORE
    //| BEGIN
    // | BETWEEN
    | BINARY
    | BLOB
    | BLOCK
    | BODY
    | BOOLEAN
    | BOTH
    // | BREADTH
    // | BY
    | BYTE
    | C_LETTER
    // | CACHE
    | CALL
	| CALLBACK
    | CASCADE
    //| CASE
    | CAST
    | CHAR
    | CHARACTER
    //| CHECK
    | CHR
    | CLOB
    | CLOSE
    | COLLECT
    | COLUMNS
    | COMMIT
    | COMMITTED
    //| CONNECT
    //| CONNECT_BY_ROOT
    | CONSTANT
    | CONSTRAINT
    | CONSTRAINTS
    | CONSTRUCTOR
    | CONTENT
    | CONTINUE
    | CONVERT
    | COST
    | COUNT
    //| CREATE
    //| CURRENT
	| CURRENT_TIME
	| CURRENT_TIMESTAMP
    | CURRENT_USER
    | CURSOR
    | CYCLE
    | DATA
    | DATABASE
    | DATE
    | DAY
    | DBTIMEZONE
    | DEC
    | DECIMAL
    //| DECLARE
    | DECREMENT
    //| DEFAULT
    | DEFAULTS
    | DEFERRED
    // | DELETE
    // | DEPTH
    //| DESC
    | DETERMINISTIC
    | DISABLE
    //| DISTINCT
    | DOUBLE
    //| DROP
    | EACH
    //| ELSE
    //| ELSIF
    | EMPTY
    | ENABLE
    //| END
    | ESCAPE
    | EVALNAME
    | EXCEPTION
    | EXCEPTION_INIT
    | EXCEPTIONS
    | EXCLUDE
    //| EXCLUSIVE
    | EXECUTE
    //| EXISTS
    | EXIT
    | EXPLAIN
    | EXTERNAL
    | EXTRACT
    //| FALSE
    //| FETCH
    | FINAL
    | FIRST
    | FIRST_VALUE
    | FLOAT
    //| FOR
    | FORALL
    // | FROM
    | FULL
    | FUNCTION
    //| GOTO
    //| GRANT
    //| GROUP
    | GROUPING
    //| HAVING
    | HOUR
    //| IF
	| IDENTIFIED
	| IDENTIFIERS
	| IDENTITY
    | IGNORE
    | IMMEDIATE
    // | IN
    | INCREMENT
    //| INDEX
    // | INNER
    | INOUT
    //| INSERT
    | INSTANTIABLE
    | INSTEAD
    | INT
    | INTEGER
    //| INTERSECT
    | INTERVAL
    // | INTO
    //| IS
    | ISOLATION
    | ITERATE
    | JOIN
    | KEEP
    | LANGUAGE
    | LAST
    | LAST_VALUE
    | LEADING
    | LEFT
    | LEVEL
    // | LIKE
    | LIMIT
    | LOCAL
	| LOCALE
    //| LOCK
    | LOCKED
    | LONG
    | LOOP
    | MAXVALUE
    | MEMBER
    | MERGE
    //| MINUS
    | MINUTE
    | MINVALUE
    //| MODE
    | MODIFY
    | MONTH
    | MULTISET
    | NAME
    | NAN
    | NATURAL
    | NATURALN
    | NAV
    | NESTED
    | NEW
	| NEXT
    | NO
    | NOAUDIT
    // | NOCACHE
    | NOCOPY
    | NOCYCLE
    //| NOMAXVALUE
    //| NOMINVALUE
    | NONE
    // | NOORDER
    //| NOT
    //| NOWAIT
    // | NULL
    | NULLS
    | NUMBER
    | NUMERIC
    | OBJECT
    //| OF
    | OFF
    | OID
    | OLD
    //| ON
    | ONLY
    | OPEN
    //| OPTION
    //| OR
    //| ORDER
	| OTHER
    | OUT
    | OUTER
    //| PERCENT_ROWTYPE
    //| PERCENT_TYPE
    //| PIVOT
    | PLAN
    | POSITIVE
    | POSITIVEN
    | PRAGMA
    | PRECEDING
    | PRECISION
    | PRESENT
    //| PRIOR
	| PRIVILEGES
    //| PROCEDURE
    | RAISE
    | RANGE
    | RAW
    | READ
    | REAL
    | RECORD
    | REF
    | REFERENCE
    | REFERENCING
	| RELATIVE
    | REPLACE
    | RESPECT
    | RESTRICT_REFERENCES
    | RESULT
    | RESULT_CACHE
    | RETURN
    | RETURNING
    | REUSE
    | REVERSE
    //| REVOKE
    | RIGHT
    | ROLLBACK
    | ROLLUP
    | ROW
    | ROWID
    | ROWS
    | RULES
    | SCHEMA
    // | SEARCH
    | SECOND
    | SEED
    // | SELECT
    | SELF
    // | SEQUENCE
    | SEQUENTIAL
    | SERIALIZABLE
	| SESSION
	| SESSIONS
    | SET
    | SETS
    | SETTINGS
    //| SHARE
    | SHOW
    | SHUTDOWN
    | SINGLE
    //| SIZE
    | SQL_SKIP
    | SMALLINT
    | SNAPSHOT
    | SOME
    | STANDALONE
    //| START
    | STARTUP
    | STATEMENT
    | STATEMENT_ID
    | STATIC
    | STATISTICS
    | STRING
    | SUCCESS
    | SUSPEND
    //| TABLE
	| TABLES
    //| THE
    //| THEN
    | TIME
    // | TIMESTAMP
    | TIMEZONE_ABBR
    | TIMEZONE_HOUR
    | TIMEZONE_MINUTE
    | TIMEZONE_REGION
    //| TO
    | TRAILING
    | TRANSACTION
    | TRANSLATE
    | TREAT
    | TRIGGER
    | TRIM
    //| TRUE
    | TRUNCATE
    | TYPE
    | UNBOUNDED
    | UNDER
    //| UNION
    //| UNIQUE
    | UNLIMITED
    //| UNPIVOT
    | UNTIL
    //| UPDATE
    | UPDATED
    | UPSERT
    | UROWID
    | USE
	| USER
	| USERTIMEZONE
    //| USING
    | VALUE
    //| VALUES
    | VARCHAR
    | VARIABLE
    | VARRAY
    | VARYING
    | VERSION
    | VERSIONS
    | WAIT
    | WARNING
    // | WHEN
    | WHENEVER
    // | WHERE
    | WHILE
    //| WITH
    | WORK
    | WRITE
    | YEAR
    | YES
    | ZONE
    | AVG
    | MAX
    | MIN
    | SUM
    ;
