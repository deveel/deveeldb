using System;

namespace Deveel.Data.Sql.Parser {
	static class SqlParsers {
		public static ISqlParser PlSql = new SqlDefaultParser(new SqlGrammar());
		public static readonly ISqlParser DataType = new SqlDefaultParser(new SqlDataTypeGrammar());
		public static readonly ISqlParser Expression = new SqlDefaultParser(new SqlExpressionGrammar());
	}
}
