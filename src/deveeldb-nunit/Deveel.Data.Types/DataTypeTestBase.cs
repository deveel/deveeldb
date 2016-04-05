using System;

using Deveel.Data.Sql.Parser;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public abstract class DataTypeTestBase {
		[TestFixtureSetUp]
		public void FixtureSetUp() {
			if (SqlParsers.DataType == null)
				SqlParsers.DataType = new SqlDefaultParser(new SqlDataTypeGrammar());
		}

		[TestFixtureTearDown]
		public void FixtureTearDown() {
			SqlParsers.DataType = null;
		}

	}
}
