using System;
using System.IO;

using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public class SqlBinaryExpressionBinarySerializeTests {
		[Test]
		public static void SerializeAdd() {
			var exp1 = SqlExpression.Add(SqlExpression.Constant(DataObject.BigInt(22)),
				SqlExpression.Constant(DataObject.Float(45.4f)));

			var stream = new MemoryStream();
			Assert.DoesNotThrow(() => SqlExpression.Serialize(exp1, stream));
			Assert.IsTrue(stream.Length > 0);
			stream.Seek(0, SeekOrigin.Begin);

			SqlExpression exp2 = null;
			Assert.DoesNotThrow(() => exp2 = SqlExpression.Deserialize(stream));
			Assert.IsNotNull(exp2);
			Assert.IsInstanceOf<SqlBinaryExpression>(exp2);
			Assert.AreEqual(exp1.ExpressionType, exp2.ExpressionType);

			var left = ((SqlBinaryExpression) exp2).Left;
			var right = ((SqlBinaryExpression) exp2).Right;

			Assert.IsInstanceOf<SqlConstantExpression>(left);
			Assert.IsInstanceOf<SqlConstantExpression>(right);

			var value1 = ((SqlConstantExpression) left).Value;
			var value2 = ((SqlConstantExpression) right).Value;

			Assert.IsInstanceOf<NumericType>(value1.Type);
			Assert.IsInstanceOf<NumericType>(value2.Type);
		}
	}
}
