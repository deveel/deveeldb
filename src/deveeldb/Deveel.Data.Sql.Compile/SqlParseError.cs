using System;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class SqlParseError {
		public SqlParseError(string message, int line, int column) {
			Message = message;
			Line = line;
			Column = column;
		}

		public string Message { get; private set; }

		public int Line { get; private set; }

		public int Column { get; private set; }
	}
}
