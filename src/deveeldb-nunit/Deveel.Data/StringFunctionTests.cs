using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class StringFunctionTests : FunctionTestBase {
		[Test]
		public void ConcatSimpleStrings() {
			var s1 = SqlExpression.Constant(Field.String("The quick"));
			var s2 = SqlExpression.Constant(Field.String(" "));
			var s3 = SqlExpression.Constant(Field.String("brown fox"));

			var result = Select("CONCAT", s1, s2, s3);

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));

			Assert.IsInstanceOf<StringType>(result.Type);
			Assert.IsInstanceOf<SqlString>(result.Value);

			var concat = result.Value.ToString();

			Assert.AreEqual("The quick brown fox", concat);
		}
	}
}
