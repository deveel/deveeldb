// 
//  Copyright 2010-2018 Deveel
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
using System.Linq;

using Deveel.Data.Sql.Parsing;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public class SqlExpressionTests : IDisposable {
		private IContext context;

		public SqlExpressionTests() {
			context = ContextUtil.NewParseContext();
		}

		[Theory]
		[InlineData("a + 56", SqlExpressionType.Add)]
		[InlineData("783.22 * 22", SqlExpressionType.Multiply)]
		[InlineData("12 / 3", SqlExpressionType.Divide)]
		[InlineData("67 - 33", SqlExpressionType.Subtract)]
		[InlineData("11 % 3", SqlExpressionType.Modulo)]
		[InlineData("a  IS NOT NULL", SqlExpressionType.IsNot)]
		[InlineData("a IS UNKNOWN", SqlExpressionType.Is)]

		//TODO: [InlineData("a IS OF TYPE VARCHAR(3)", SqlExpressionType.Is)]
		[InlineData("c > 22", SqlExpressionType.GreaterThan)]
		[InlineData("178.999 >= 902", SqlExpressionType.GreaterThanOrEqual)]
		[InlineData("a < -1", SqlExpressionType.LessThan)]
		[InlineData("189 <= 189", SqlExpressionType.LessThanOrEqual)]
		[InlineData("a = b", SqlExpressionType.Equal)]
		[InlineData("a <> c", SqlExpressionType.NotEqual)]
		[InlineData("TRUE OR FALSE", SqlExpressionType.Or)]
		[InlineData("a AND b", SqlExpressionType.And)]

		//TODO: [InlineData("674 XOR 90", SqlExpressionType.XOr)]
		[InlineData("a BETWEEN 56 AND 98", SqlExpressionType.And)]
		public void ParseBinaryString(string s, SqlExpressionType expressionType) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.Equal(expressionType, exp.ExpressionType);
			Assert.IsType<SqlBinaryExpression>(exp);
		}

		[Theory]
		[InlineData("CAST(54 AS BIGINT)", "BIGINT")]
		[InlineData("CAST('test' AS VARCHAR(255))", "VARCHAR(255)")]
		public void ParseCastString(string s, string castTypeString) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlCastExpression>(exp);

			var cast = (SqlCastExpression) exp;

			var castType = SqlType.Parse(context, castTypeString);
			Assert.Equal(castType, cast.TargetType);
		}

		[Theory]
		[InlineData("CASE a WHEN 1 THEN TRUE ELSE FALSE END")]
		[InlineData("CASE WHEN a = 1 THEN TRUE ELSE FALSE END")]
		[InlineData("CASE a WHEN 1 THEN TRUE WHEN 2 THEN TRUE ELSE FALSE END")]
		[InlineData("CASE WHEN a = 1 THEN TRUE WHEN b = 2 THEN FALSE END")]
		public void ParseSimpleCaseString(string s) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlConditionExpression>(exp);

			var condition = (SqlConditionExpression) exp;

			Assert.NotNull(condition.Test);
			Assert.NotNull(condition.IfTrue);
			Assert.NotNull(condition.IfFalse);
		}

		[Theory]
		[InlineData("TRUE")]
		[InlineData("FALSE")]
		[InlineData("UNKNOWN")]
		[InlineData("NULL")]
		[InlineData("'test string'")]
		[InlineData("7859403.112")]
		public void ParseConstantString(string s) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlConstantExpression>(exp);

			var constantExp = (SqlConstantExpression) exp;
			Assert.NotNull(constantExp.Value);
			Assert.NotNull(constantExp.Value.Type);
		}

		[Theory]
		[InlineData("sys.Func1(a => 56)", "sys.Func1", "a", "INT", 56)]
		[InlineData("fun2(a => 'test')", "fun2", "a", "STRING", "test")]
		public void ParseFunctionWithNamedParameter(string s, string funcName, string paramName, string paramType,
			object paramValue) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.Equal(SqlExpressionType.Function, exp.ExpressionType);
			Assert.IsType<SqlFunctionExpression>(exp);

			var function = (SqlFunctionExpression) exp;

			Assert.Equal(ObjectName.Parse(funcName), function.FunctionName);
			Assert.NotEmpty(function.Arguments);
			Assert.Single(function.Arguments);

			var param = function.Arguments[0];

			Assert.NotNull(param);
			Assert.True(param.IsNamed);
			Assert.Equal(paramName, param.Name);
			Assert.IsType<SqlConstantExpression>(param.Value);

			var type = SqlType.Parse(context, paramType);
			var value = new SqlObject(type, SqlValueUtil.FromObject(paramValue));

			Assert.Equal(value, ((SqlConstantExpression) param.Value).Value);
		}

		[Theory]
		[InlineData("sys.Func1(56, 'test1')", "sys.Func1", "INT", 56, "STRING", "test1")]
		public void ParseFunctionWithAnonParameter(string s, string funcName, string param1Type,
			object paramValue1, string param2Type, object paramValue2) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.Equal(SqlExpressionType.Function, exp.ExpressionType);
			Assert.IsType<SqlFunctionExpression>(exp);

			var function = (SqlFunctionExpression) exp;

			Assert.Equal(ObjectName.Parse(funcName), function.FunctionName);
			Assert.NotEmpty(function.Arguments);
			Assert.Equal(2, function.Arguments.Length);

			var param1 = function.Arguments[0];
			var param2 = function.Arguments[1];

			Assert.NotNull(param1);
			Assert.NotNull(param2);

			Assert.False(param1.IsNamed);
			Assert.False(param2.IsNamed);

			Assert.IsType<SqlConstantExpression>(param1.Value);
			Assert.IsType<SqlConstantExpression>(param2.Value);

			var pType1 = SqlType.Parse(context, param1Type);
			var pType2 = SqlType.Parse(context, param2Type);

			var value1 = new SqlObject(pType1, SqlValueUtil.FromObject(paramValue1));
			var value2 = new SqlObject(pType2, SqlValueUtil.FromObject(paramValue2));

			Assert.Equal(value1, ((SqlConstantExpression) param1.Value).Value);
			Assert.Equal(value2, ((SqlConstantExpression) param2.Value).Value);
		}

		[Theory]
		[InlineData("TRIM (LEADING ' ' FROM '  test')")]
		[InlineData("TRIM(BOTH ' ' FROM ' test ')")]
		[InlineData("TRIM(' test')")]
		[InlineData("TRIM(TRAILING ' ' FROM 'test ')")]
		public void ParseSqlTrimFunction(string s) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlFunctionExpression>(exp);

			var func = (SqlFunctionExpression) exp;
			Assert.Equal("SQL_TRIM", func.FunctionName.FullName);
			Assert.NotEmpty(func.Arguments);
			Assert.Equal(3, func.Arguments.Length);
		}

		[Theory]
		[InlineData("EXTRACT(DAY FROM '1980-06-04')")]
		[InlineData("EXTRACT(YEAR FROM birth_date)")]
		public void ParseSqlExtractFunction(string s) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlFunctionExpression>(exp);

			var func = (SqlFunctionExpression) exp;
			Assert.Equal("SQL_EXTRACT", func.FunctionName.FullName);
			Assert.NotEmpty(func.Arguments);
			Assert.Equal(2, func.Arguments.Length);
		}

		[Theory]
		[InlineData("CURRENT_TIME", "TIME")]
		[InlineData("CURRENT_TIMESTAMP", "TIMESTAMP")]
		[InlineData("CURRENT_DATE", "DATE")]
		[InlineData("DBTIMEZONE", "DBTIMEZONE")]
		[InlineData("USERTIMEZONE", "UserTimeZone")]
		public void ParseSqlSystemFunction(string s, string functionName) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlFunctionExpression>(exp);

			var func = (SqlFunctionExpression) exp;
			Assert.Equal(functionName, func.FunctionName.FullName);
		}


		[Theory]
		[InlineData("TIMESTAMP '1980-04-06'")]
		[InlineData("TIMESTAMP :a")]
		public void ParseTimeStampFunction(string s) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlFunctionExpression>(exp);

			var func = (SqlFunctionExpression) exp;
			Assert.Equal("TOTIMESTAMP", func.FunctionName.FullName);
			Assert.NotEmpty(func.Arguments);
			Assert.Single(func.Arguments);
		}

		[Theory]
		[InlineData("TIMESTAMP '2017-02-01T09:12:02.4533' AT TIME ZONE 'CET'")]
		private void ParseAdvancedTimeStampFunction(string s) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlFunctionExpression>(exp);

			var func = (SqlFunctionExpression) exp;
			Assert.Equal("TOTIMESTAMP", func.FunctionName.FullName);
			Assert.NotEmpty(func.Arguments);
			Assert.Equal(2, func.Arguments.Length);
		}

		[Theory]
		[InlineData("DATE '1980-04-06'")]
		public void ParseDateFunction(string s) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlFunctionExpression>(exp);

			var func = (SqlFunctionExpression) exp;
			Assert.Equal("CAST", func.FunctionName.FullName);
			Assert.NotEmpty(func.Arguments);
			Assert.Equal(2, func.Arguments.Length);
		}

		[Theory]
		[InlineData("(674)", SqlExpressionType.Constant)]
		[InlineData("((a + b))", SqlExpressionType.Group)]
		public void ParseGroupString(string s, SqlExpressionType innerType) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlGroupExpression>(exp);

			var group = (SqlGroupExpression) exp;

			Assert.NotNull(group.Expression);
			Assert.Equal(innerType, group.Expression.ExpressionType);
		}

		[Fact]
		public void ParseSimpleQuery() {
			const string sql = "SELECT * FROM app.a a_table WHERE a.id > 4";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.NotNull(query.Where);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
		}

		[Fact]
		public void ParseJoinedQuery() {
			const string sql = "SELECT a.id, b.* FROM a INNER JOIN b ON a.id = b.a_id WHERE b.level >= 3";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(2, query.Items.Count);
			Assert.NotNull(query.Where);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Equal(2, query.From.Sources.Count());
		}

		[Fact]
		public void ParseSubquery() {
			const string sql = "SELECT * FROM (SELECT a FROM b) q WHERE q.a > 5";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
			Assert.NotNull(query.Where);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Single(query.From.Sources);
			Assert.IsType<SqlQueryExpressionSource>(query.From.Sources.First());
		}

		[Fact]
		public void ParseGroupByQuery() {
			const string sql = "SELECT COUNT(*) FROM a GROUP BY a.b";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
			Assert.Null(query.Where);
			Assert.NotNull(query.GroupBy);
			Assert.NotEmpty(query.GroupBy);
			Assert.Equal(1, query.GroupBy.Count);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Single(query.From.Sources);
		}

		[Fact]
		public void ParseGroupMaxQuery() {
			const string sql = "SELECT COUNT(*) FROM a GROUP MAX a.b";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
			Assert.Null(query.Where);
			Assert.NotNull(query.GroupMax);
			Assert.NotNull(query.From);
			Assert.NotEmpty(query.From.Sources);
			Assert.Single(query.From.Sources);
		}

		[Fact]
		public void ParseSelectInArray() {
			const string sql = "SELECT * FROM a WHERE a.b IN (45, 893, 001)";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}

		[Fact]
		public void ParseSelectInQuery() {
			const string sql = "SELECT * FROM a WHERE a.b IN (SELECT a FROM b)";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}

		[Fact]
		public void ParseSelectInVariable() {
			const string sql = "SELECT * FROM a WHERE a.b IN :b";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}

		[Fact]
		public void ParseSelectInto() {
			const string sql = "SELECT * INTO :a1 FROM a WHERE a.id = 22";

			var exp = SqlExpression.Parse(context, sql);

			Assert.NotNull(exp);
			Assert.IsType<SqlQueryExpression>(exp);

			var query = (SqlQueryExpression) exp;
			Assert.NotEmpty(query.Items);
			Assert.Equal(1, query.Items.Count);
		}

		[Theory]
		[InlineData("a LIKE 'anto%'", SqlExpressionType.Like)]
		[InlineData("a NOT LIKE '%hell%'", SqlExpressionType.NotLike)]
		public void ParseStringMatch(string s, SqlExpressionType expressionType) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlStringMatchExpression>(exp);
			Assert.Equal(expressionType, exp.ExpressionType);
		}

		[Theory]
		[InlineData("+454.90", SqlExpressionType.UnaryPlus)]
		[InlineData("NOT TRUE", SqlExpressionType.Not)]
		[InlineData("-7849", SqlExpressionType.Negate)]
		public void ParseUnaryString(string s, SqlExpressionType expressionType) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.Equal(expressionType, exp.ExpressionType);
			Assert.IsType<SqlUnaryExpression>(exp);
		}

		[Theory]
		[InlineData(":a", "a")]
		[InlineData(":1", "1")]
		public void ParseVariableString(string s, string varName) {
			var exp = SqlExpression.Parse(context, s);

			Assert.NotNull(exp);
			Assert.IsType<SqlVariableExpression>(exp);

			var varExp = (SqlVariableExpression) exp;
			Assert.NotNull(varExp.VariableName);
			Assert.Equal(varName, varExp.VariableName);
		}


		public void Dispose() {
			context?.Dispose();
		}
	}
}