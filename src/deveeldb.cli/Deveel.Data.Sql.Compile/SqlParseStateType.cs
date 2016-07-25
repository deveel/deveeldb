using System;

namespace Deveel.Data.Sql.Compile {
	public enum SqlParseStateType {
		Unknown = -1,
		NewStatement = 0,
		Start = 1, // statement == start
		Statement = 1,
		StartComment = 3,
		Comment = 4,
		PreEndComment = 5,
		StartAnsiString = 6,
		EndLineComment = 7,
		String = 8,
		StringQuote = 9,
		SqlString = 10,
		SqlStringQuote = 11,
		StatementQuote = 12, // backslash in statement
		FirstSemicolonOnLine = 13,
		PotentialEndFound = 14,
	}
}