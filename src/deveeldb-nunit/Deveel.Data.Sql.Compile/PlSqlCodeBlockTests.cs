using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class PlSqlCodeBlockTests : SqlCompileTestBase {
		[Test]
		public void SelectInBlock() {
			const string sql = @"BEGIN
							SELECT * FROM test WHERE a = 90 AND
													 b > 12.922;
						END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var obj = result.Statements.ElementAt(0);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<PlSqlBlockStatement>(obj);

			var block = (PlSqlBlockStatement) obj;

			Assert.AreEqual(1, block.Statements.Count);
			Assert.AreEqual(0, block.ExceptionHandlers.Count());
			Assert.IsNull(block.Label);

			var statement = block.Statements.ElementAt(0);
			Assert.IsNotNull(statement);
			Assert.IsInstanceOf<SelectStatement>(statement);
		}

		[Test]
		public void DeclarationsBeforeBlock() {
			const string sql = @"DECLARE a INT := 23
								BEGIN
									SELECT * FROM test WHERE a < test.a;
								END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);

			Assert.IsInstanceOf<PlSqlBlockStatement>(statement);
		}
	}
}
