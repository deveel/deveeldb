using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class LoopTests : SqlCompileTestBase {
		[Test]
		public void EmptyLoop() {
			const string sql = @"LOOP
									IF a = 33 THEN
                                      EXIT;
                                    END IF 
								 END LOOP";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.Statements.Count);

			var statement = result.Statements.ElementAt(0);
			Assert.IsInstanceOf<LoopStatement>(statement);
		}
	}
}
