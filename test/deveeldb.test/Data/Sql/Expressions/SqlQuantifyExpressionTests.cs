using System;
using System.Threading.Tasks;

using Deveel.Data.Query;
using Deveel.Data.Serialization;
using Deveel.Data.Services;
using Deveel.Data.Sql.Types;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Expressions {
	public class SqlQuantifyExpressionTests : IDisposable {
		private IQuery context;
		private SqlArray array;

		public SqlQuantifyExpressionTests() {
			var mock = new Mock<IQuery>();
			mock.As<IContext>().SetupGet(x => x.Scope)
				.Returns(new ServiceContainer());

			context = mock.Object;

			array = new SqlArray(new SqlExpression[] {
				SqlExpression.Constant(SqlObject.New((SqlNumber)2203.112)),
				SqlExpression.Constant(SqlObject.New((SqlNumber)32)),
				SqlExpression.Constant(SqlObject.New((SqlNumber)0.9923))
			});
		}

		[Theory]
		[InlineData(SqlExpressionType.Any, SqlExpressionType.GreaterThan)]
		[InlineData(SqlExpressionType.All, SqlExpressionType.LessThan)]
		public void CreateQuantify(SqlExpressionType expressionType, SqlExpressionType opType) {
			var left = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(212)));
			var right = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(array)));

			var binary = SqlExpression.Binary(opType, left, right);
			var exp = SqlExpression.Quantify(expressionType, binary);

			Assert.Equal(expressionType, exp.ExpressionType);
			Assert.Equal(opType, exp.Expression.ExpressionType);
		}

		[Theory]
		[InlineData(SqlExpressionType.Any, SqlExpressionType.Divide)]
		[InlineData(SqlExpressionType.All, SqlExpressionType.Add)]
		public void CreateInvalidQuantify(SqlExpressionType expressionType, SqlExpressionType opType) {
			var left = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(212)));
			var right = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(array)));

			var binary = SqlExpression.Binary(opType, left, right);
			Assert.Throws<ArgumentException>(() => SqlExpression.Quantify(expressionType, binary));
		}

		[Theory]
		[InlineData(SqlExpressionType.Any, SqlExpressionType.GreaterThan, "212 > ANY(2203.112, 32, 0.9923)")]
		[InlineData(SqlExpressionType.All, SqlExpressionType.LessThan, "212 < ALL(2203.112, 32, 0.9923)")]
		public void GetQuantifyString(SqlExpressionType expressionType, SqlExpressionType opType, string expected) {
			var left = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(212)));
			var right = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(array)));

			var binary = SqlExpression.Binary(opType, left, right);
			var exp = SqlExpression.Quantify(expressionType, binary);

			var sql = exp.ToString();
			Assert.Equal(expected, sql);
		}

		[Theory]
		[InlineData(45, SqlExpressionType.GreaterThan, SqlExpressionType.Any, true)]
		[InlineData(22, SqlExpressionType.LessThan, SqlExpressionType.Any, true)]
		[InlineData(2203.112, SqlExpressionType.Equal, SqlExpressionType.Any, true)]
		[InlineData(22, SqlExpressionType.Equal, SqlExpressionType.Any, false)]
		[InlineData(3224, SqlExpressionType.NotEqual, SqlExpressionType.Any, true)]
		[InlineData(1203994053, SqlExpressionType.GreaterThan, SqlExpressionType.Any, true)]
		[InlineData(32, SqlExpressionType.GreaterThanOrEqual, SqlExpressionType.Any, true)]
		[InlineData(0.02, SqlExpressionType.GreaterThanOrEqual, SqlExpressionType.Any, false)]
		[InlineData(32, SqlExpressionType.LessThanOrEqual, SqlExpressionType.Any, true)]

		[InlineData(45, SqlExpressionType.GreaterThan, SqlExpressionType.All, false)]
		[InlineData(22, SqlExpressionType.LessThan, SqlExpressionType.All, false)]
		[InlineData(2203.112, SqlExpressionType.Equal, SqlExpressionType.All, false)]
		[InlineData(22, SqlExpressionType.Equal, SqlExpressionType.All, false)]
		[InlineData(3224, SqlExpressionType.NotEqual, SqlExpressionType.All, true)]
		[InlineData(1203994053, SqlExpressionType.GreaterThan, SqlExpressionType.All, true)]
		[InlineData(32, SqlExpressionType.GreaterThanOrEqual, SqlExpressionType.All, false)]
		[InlineData(0.02, SqlExpressionType.GreaterThanOrEqual, SqlExpressionType.All, false)]
		[InlineData(32, SqlExpressionType.LessThanOrEqual, SqlExpressionType.All, false)]
		public async Task Quantify(object value, SqlExpressionType opType, SqlExpressionType expressionType, bool expected) {
			var left = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(SqlValueUtil.FromObject(value))));
			var right = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(array)));

			var binary = SqlExpression.Binary(opType, left, right);
			var exp = SqlExpression.Quantify(expressionType, binary);

			var result = await exp.ReduceAsync(context);

			Assert.NotNull(result);
			Assert.Equal(SqlExpressionType.Constant, result.ExpressionType);

			var resultValue = ((SqlConstantExpression) result).Value;

			Assert.IsType<SqlBooleanType>(resultValue.Type);
			Assert.IsType<SqlBoolean>(resultValue.Value);

			Assert.Equal(expected, (bool) ((SqlBoolean)resultValue.Value));
		}

		//[Theory]
		//[InlineData(SqlExpressionType.Any, SqlExpressionType.GreaterThan)]
		//[InlineData(SqlExpressionType.All, SqlExpressionType.LessThan)]
		//public void SerializeQuantify(SqlExpressionType expressionType, SqlExpressionType opType) {
		//	var left = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(212)));
		//	var right = SqlExpression.Constant(SqlObject.New(SqlValueUtil.FromObject(array)));

		//	var binary = SqlExpression.Binary(opType, left, right);
		//	var exp = SqlExpression.Quantify(expressionType, binary);

		//	var result = BinarySerializeUtil.Serialize(exp);

		//	Assert.IsType<SqlBinaryExpression>(result.Expression);
		//	Assert.Equal(expressionType, result.ExpressionType);
		//	Assert.Equal(opType, result.Expression.ExpressionType);
		//}

		public void Dispose() {
			if (context != null)
				context.Dispose();
		}
	}
}