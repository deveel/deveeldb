using System;

namespace Deveel.Data.Sql.Compile {
	public static class SqlParsers {
		public static ISqlParser Default = new SqlDefaultParser(new SqlGrammar());
		public static ISqlParser Expression = new SqlDefaultParser(new SqlExpressionGrammar());
		public static ISqlParser DataType = new SqlDefaultParser(new SqlDataTypeGrammar());
	}
}
