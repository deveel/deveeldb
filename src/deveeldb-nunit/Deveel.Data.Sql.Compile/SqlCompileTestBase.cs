using System;

using Deveel.Data.Sql.Parser;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public abstract class SqlCompileTestBase {
		[TestFixtureSetUp]
		public void SetUp() {
			SqlDefaultCompiler.Init();
		}

		protected SqlCompileResult Compile(string sql) {
			var compiler=new SqlDefaultCompiler();
			return compiler.Compile(new SqlCompileContext(sql));
		}
	}
}
