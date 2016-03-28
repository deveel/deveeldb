using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ConversionFunctionTests : FunctionTestBase {
		[Test]
		public void ToString_Integer() {
			var value = SqlExpression.Constant(Field.Integer(455366));
			var result = Select("TOSTRING", value);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("455366", stringResult);
		}

		[Test]
		public void ToString_Date() {
			var value = SqlExpression.Constant(Field.Date(new SqlDateTime(2015, 02, 10)));
			var result = Select("TOSTRING", value);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("2015-02-10", stringResult);
		}

		[Test]
		public void ToString_TimeStamp_NoFormat() {
			var value = SqlExpression.Constant(Field.TimeStamp(new SqlDateTime(2015, 02, 10, 17, 15, 01, 00)));
			var result = Select("TOSTRING", value);
			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("2015-02-10T17:15:01.000 +00:00", stringResult);
		}

		[Test]
		public void Cast_NumberToVarChar() {
			var value = SqlExpression.Constant(Field.Integer(455366));
			var type = SqlExpression.Constant(Field.String("VARCHAR(255)"));

			var result = Select("CAST", value, type);

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<StringType>(result.Type);

			var stringResult = result.Value.ToString();
			Assert.AreEqual("455366", stringResult);
		}
	}
}
