using System;
using System.IO;

namespace Deveel.Data.Sql {
	public sealed class SqlParseException : Exception {
		internal SqlParseException(ParseException ex, string source) {
			this.ex = ex;
			this.source = source;
			startLine = ex.currentToken.beginLine;
			endLine = ex.currentToken.endLine;
			startColumn = ex.currentToken.beginColumn;
			endColumn = ex.currentToken.endColumn;
			expected = ex.tokenImage;
		}

		private readonly int startLine;
		private readonly int endLine;
		private readonly int startColumn;
		private readonly int endColumn;
		private readonly string source;
		private readonly string[] expected;
		private ParseException ex;

		public string[] Expected {
			get { return expected; }
		}

		public string ParseSource {
			get { return source; }
		}

		public int EndColumn {
			get { return endColumn; }
		}

		public int StartColumn {
			get { return startColumn; }
		}

		public int EndLine {
			get { return endLine; }
		}

		public int StartLine {
			get { return startLine; }
		}

		public override string Message {
			get { return GenerateMessage(); }
		}

		private string GenerateMessage() {
			//TODO:!!!
			return ex.Message;
		}
	}
}