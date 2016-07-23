using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

using NUnit.Framework;

namespace Deveel.Data.Sql.Statements {
	[TestFixture]
	public static class CallStringFormatTests {
		[Test]
		public static void NoArgs() {
			var statement = new CallStatement(ObjectName.Parse("proc1"));

			var sql = statement.ToString();

			Assert.AreEqual("CALL proc1()", sql);
		}

		[Test]
		public static void AnonArgs() {
			var statement = new CallStatement(ObjectName.Parse("proc1"), new SqlExpression[] {
				SqlExpression.Constant("one")
			});

			var sql = statement.ToString();

			Assert.AreEqual("CALL proc1('one')", sql);
		}

		[Test]
		public static void NamedArgs() {
			var statement = new CallStatement(ObjectName.Parse("APP.proc1"), new InvokeArgument[] {
				new InvokeArgument("a", SqlExpression.Constant(Field.Number(new SqlNumber(8399.22, 6))))
			});

			var sql = statement.ToString();

			Assert.AreEqual("CALL APP.proc1(a => 8399.22)", sql);
		}
	}
}
