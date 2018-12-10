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
using System.Reflection;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using Xunit;

namespace Deveel.Data.Sql {
	public static class SqlQueryTests {
		[Fact]
		public static void NewSimpleQuery() {
			var query = new SqlCommand("SELECT * FROM b");

			Assert.Equal("SELECT * FROM b", query.Text);
			Assert.NotNull(query.Parameters);
			Assert.Empty(query.Parameters);
			Assert.Equal(SqlParameterNaming.Default, query.ParameterNaming);
		}

		[Fact]
		public static void NewQueryWithNamedParameter() {
			var query = new SqlCommand("INSERT INTO a (col1) VALUES (:v1)", SqlParameterNaming.Named);
			query.Parameters.Add(new SqlParameter("v1", PrimitiveTypes.Boolean(), SqlBoolean.True));

			Assert.Equal("INSERT INTO a (col1) VALUES (:v1)", query.Text);
			Assert.Equal(SqlParameterNaming.Named, query.ParameterNaming);
			Assert.NotNull(query.Parameters);
			Assert.NotEmpty(query.Parameters);
			Assert.Equal(1, query.Parameters.Count);

			var param = query.Parameters.ElementAt(0);
			Assert.NotNull(param);
			Assert.Equal("v1", param.Name);
			Assert.NotNull(param.SqlType);
			Assert.IsType<SqlBooleanType>(param.SqlType);
			Assert.Equal(SqlParameterDirection.In, param.Direction);
			Assert.IsType<SqlBoolean>(param.Value);
			Assert.Equal(SqlBoolean.True, param.Value);
		}

		[Theory]
		[InlineData(SqlParameterNaming.Marker, ":var1")]
		[InlineData(SqlParameterNaming.Named, "?")]
		public static void AddInvalidStyledQueryParameter(SqlParameterNaming naming, string paramName) {
			var query = new SqlCommand($"INSERT INTO a (col1) VALUES ({paramName})", naming);
			Assert.Throws<ArgumentException>(() => query.Parameters.Add(new SqlParameter(paramName, PrimitiveTypes.Boolean(), SqlBoolean.True)));
		}

		[Theory]
		[InlineData("a")]
		[InlineData(":a")]
		public static void AddDoubleNamedQueryParameter(string paramName) {
			var query = new SqlCommand($"INSERT INTO a (col1) VALUES ({paramName})", SqlParameterNaming.Named);
			query.Parameters.Add(new SqlParameter(paramName, PrimitiveTypes.Boolean(), SqlBoolean.True));
			Assert.Throws<ArgumentException>(
				() => query.Parameters.Add(new SqlParameter(paramName, PrimitiveTypes.Boolean(), SqlBoolean.True)));
		}

		[Theory]
		[InlineData(SqlParameterNaming.Marker, "?", "?")]
		[InlineData(SqlParameterNaming.Named, ":var1", ":var2")]
		public static void ChangeStyle(SqlParameterNaming newNaming, string paramName1, string paramName2) {
			var query = new SqlCommand($"SELECT * FROM a WHERE a.col1 = {paramName1} AND a.col2 = {paramName2}");
			query.Parameters.Add(new SqlParameter(paramName1, PrimitiveTypes.String(),new SqlString($"val of {paramName1}")));
			query.Parameters.Add(new SqlParameter(paramName2, PrimitiveTypes.Boolean(), SqlBoolean.False));

			Assert.Equal(SqlParameterNaming.Default, query.ParameterNaming);
			ChangeNamingOf(query, newNaming);
			Assert.Equal(newNaming, query.ParameterNaming);
		}

		[Theory]
		[InlineData(SqlParameterNaming.Marker, "?", "var1")]
		[InlineData(SqlParameterNaming.Named, ":var1", "?")]
		public static void ChangeStyleWithInvalidParameters(SqlParameterNaming newNaming, string paramName1, string paramName2) {
			var query = new SqlCommand($"SELECT * FROM a WHERE a.col1 = {paramName1} AND a.col2 = {paramName2}");
			query.Parameters.Add(new SqlParameter(paramName1, PrimitiveTypes.String(), new SqlString($"val of {paramName1}")));
			query.Parameters.Add(new SqlParameter(paramName2, PrimitiveTypes.Boolean(), SqlBoolean.False));

			Assert.Equal(SqlParameterNaming.Default, query.ParameterNaming);

			Assert.Throws<ArgumentException>(() => ChangeNamingOf(query, newNaming));
		}

		[Theory]
		[InlineData(SqlParameterNaming.Marker)]
		[InlineData(SqlParameterNaming.Named)]
		public static void ChangeStyleOfNonDefault(SqlParameterNaming naming) {
			var query = new SqlCommand("EXECUTE sp_insertData", naming);
			Assert.Throws<InvalidOperationException>(() => ChangeNamingOf(query, naming));
		}

		private static void ChangeNamingOf(SqlCommand command, SqlParameterNaming newNaming) {
			try {
				typeof(SqlCommand).GetMethod("ChangeNaming", BindingFlags.Instance | BindingFlags.NonPublic)
					.Invoke(command, new object[] {newNaming});
			} catch(TargetInvocationException ex) {
				throw ex.InnerException;
			}
		}


		[Theory]
		[InlineData(324, 112.022)]
		public static void PrepareMarkerParameters(object value1, object value2) {
			var input1 = SqlObject.New(SqlValueUtil.FromObject(value1));
			var input2 = SqlObject.New(SqlValueUtil.FromObject(value2));

			var query = new SqlCommand("SELECT * FROM a WHERE a.col1 = ? AND a.col2 = ?", SqlParameterNaming.Marker);
			query.Parameters.Add(new SqlParameter("?", input1));
			query.Parameters.Add(new SqlParameter("?", input2));

			var preparer = query.ExpressionPreparer;

			Assert.NotNull(preparer);

			var param1 = SqlExpression.Parameter();
			var param2 = SqlExpression.Parameter();

			Assert.True(preparer.CanPrepare(param1));
			Assert.True(preparer.CanPrepare(param2));

			var exp1 = preparer.Prepare(param1);
			var exp2 = preparer.Prepare(param2);

			Assert.NotNull(exp1);
			Assert.NotNull(exp2);

			Assert.IsType<SqlConstantExpression>(exp1);
			Assert.IsType<SqlConstantExpression>(exp2);

			Assert.Equal(input1, ((SqlConstantExpression)exp1).Value);
			Assert.Equal(input2, ((SqlConstantExpression)exp2).Value);
		}

		[Theory]
		[InlineData("var1", 324, "var2", 112.022)]
		public static void PrepareNamedParameters(string paramName1, object value1, string paramName2, object value2) {
			var input1 = SqlObject.New(SqlValueUtil.FromObject(value1));
			var input2 = SqlObject.New(SqlValueUtil.FromObject(value2));

			var query = new SqlCommand($"SELECT * FROM a WHERE a.col1 = {paramName1} AND a.col2 = {paramName2}", SqlParameterNaming.Named);
			query.Parameters.Add(new SqlParameter(paramName1, input1));
			query.Parameters.Add(new SqlParameter(paramName2, input2));

			var preparer = query.ExpressionPreparer;

			Assert.NotNull(preparer);

			var param1 = SqlExpression.Variable(paramName1);
			var param2 = SqlExpression.Variable(paramName2);

			Assert.True(preparer.CanPrepare(param1));
			Assert.True(preparer.CanPrepare(param2));

			var exp1 = preparer.Prepare(param1);
			var exp2 = preparer.Prepare(param2);

			Assert.NotNull(exp1);
			Assert.NotNull(exp2);

			Assert.IsType<SqlConstantExpression>(exp1);
			Assert.IsType<SqlConstantExpression>(exp2);

			Assert.Equal(input1, ((SqlConstantExpression)exp1).Value);
			Assert.Equal(input2, ((SqlConstantExpression)exp2).Value);
		}

	}
}