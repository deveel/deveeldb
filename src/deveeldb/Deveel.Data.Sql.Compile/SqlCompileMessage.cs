using System;

namespace Deveel.Data.Sql.Compile {
	public sealed class SqlCompileMessage {
		public SqlCompileMessage(CompileMessageLevel level, string text) 
			: this(level, text, null) {
		}

		public SqlCompileMessage(CompileMessageLevel level, string text, SourceLocation location) {
			if (String.IsNullOrEmpty(text))
				throw new ArgumentNullException(nameof(text));

			Level = level;
			Text = text;
			Location = location;
		}

		public CompileMessageLevel Level { get; private set; }

		public string Text { get; private set; }

		public SourceLocation Location { get; private set; }
	}
}