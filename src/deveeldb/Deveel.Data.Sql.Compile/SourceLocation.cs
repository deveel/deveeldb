using System;

namespace Deveel.Data.Sql.Compile {
	public sealed class SourceLocation {
		public SourceLocation(int line, int column) {
			Line = line;
			Column = column;
		}

		public int Line { get; private set; }

		public int Column { get; private set; }
	}
}
