// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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