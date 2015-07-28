using System;
using System.IO;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	[TestFixture]
	public class SqlFunctionCallExpressionTests {
		[Test]
		public static void SerializeSimple() {
			var exp1 = SqlExpression.FunctionCall("count", new SqlExpression[] { SqlExpression.Reference(new ObjectName("*")) });

			var stream = new MemoryStream();
			Assert.DoesNotThrow(() => SqlExpression.Serialize(exp1, stream));
			Assert.IsTrue(stream.Length > 0);
			stream.Seek(0, SeekOrigin.Begin);

			SqlExpression exp2 = null;
			Assert.DoesNotThrow(() => exp2 = SqlExpression.Deserialize(stream));
			Assert.IsNotNull(exp2);
			Assert.IsInstanceOf<SqlFunctionCallExpression>(exp2);
			Assert.AreEqual(exp1.ExpressionType, exp2.ExpressionType);
		}
	}
}
