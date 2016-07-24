using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class DeclareVariableStringFormatTests {
		[Test]
		public static void ConstantInteger() {
			var statement = new DeclareVariableStatement("a", PrimitiveTypes.Integer()) {
				IsConstant = true
			};

			var sql = statement.ToString();
			var expected = "CONSTANT a INTEGER";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void NotNullString() {
			var statement = new DeclareVariableStatement("a", PrimitiveTypes.VarChar(30)) {
				IsNotNull = true
			};

			var sql = statement.ToString();
			var expected = "a VARCHAR(30) NOT NULL";

			Assert.AreEqual(expected, sql);
		}

		[Test]
		public static void BigIntWithDefault() {
			var statement = new DeclareVariableStatement("c", PrimitiveTypes.BigInt()) {
				DefaultExpression =
					SqlExpression.Multiply(SqlExpression.Constant(56), SqlExpression.Reference(new ObjectName("test.a")))
			};

			var sql = statement.ToString();
			var expected = "c BIGINT := 56 * test.a";

			Assert.AreEqual(expected, sql);
		}
	}
}
