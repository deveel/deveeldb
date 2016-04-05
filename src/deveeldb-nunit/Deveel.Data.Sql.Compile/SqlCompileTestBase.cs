using System;

using Deveel.Data.Sql.Parser;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public abstract class SqlCompileTestBase {
		[TestFixtureSetUp]
		public void FixtureSetUp() {
			if (SqlParsers.PlSql == null)
				SqlParsers.PlSql = new SqlDefaultParser(new SqlGrammar());
		}

		[TestFixtureTearDown]
		public void FixtureTearDown() {
			SqlParsers.PlSql = null;
		}


		protected SqlCompileResult Compile(string sql) {
			var compiler=new SqlDefaultCompiler();
			return compiler.Compile(new SqlCompileContext(sql));
		}
	}
}
