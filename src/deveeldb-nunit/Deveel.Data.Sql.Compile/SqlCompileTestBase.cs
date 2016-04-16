using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public abstract class SqlCompileTestBase {
		protected SqlCompileResult Compile(string sql) {
			var compiler=new PlSqlCompiler();
			return compiler.Compile(new SqlCompileContext(sql));
		}

		[TestFixtureTearDown]
		public void TestFixtureTearDown() {
			GC.Collect();
			GC.WaitForFullGCComplete(-1);
			GC.WaitForPendingFinalizers();
		}
	}
}
