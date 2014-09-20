// 
//  Copyright 2010-2014 Deveel
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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class SqlParseException : Exception {
		internal SqlParseException(ParseException ex, string source, List<Token> allTokens) {
			this.ex = ex;
			this.source = source;
			this.allTokens = allTokens;
			startLine = ex.currentToken.beginLine;
			endLine = ex.currentToken.endLine;
			startColumn = ex.currentToken.beginColumn;
			endColumn = ex.currentToken.endColumn;
			expected = BuildExpected(ex);
		}

		private readonly List<Token> allTokens;
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
			var nextTokens = new List<string>();

			var prevTokens =
				allTokens.TakeWhile(t => t.beginLine != ex.currentToken.beginLine || t.beginColumn != ex.currentToken.beginColumn)
					.Select(t => t.image)
					.ToList();

			if (prevTokens.Count > 7) {
				var temp = new string[8];
				prevTokens.CopyTo(prevTokens.Count - 7, temp, 1, 7);
				temp[0] = "...";
				prevTokens = new List<string>(temp);
			}

			var token = ex.currentToken.next;
			while (token != null) {
				nextTokens.Add(token.image);
				token = token.next;
			}

			var s = new StringBuilder();
			if (prevTokens.Count > 0) {
				s.Append(String.Join(" ", prevTokens.ToArray()));
				s.Append(" ");
			}

			s.Append(ex.currentToken.image);

			if (nextTokens.Count > 0) {
				s.Append(" ");
				s.Append(String.Join(" ", nextTokens.ToArray()));
			}

			var expectedString = String.Join(", ", expected);
			if (expected.Length == 1) {
				expectedString = "expected " + expectedString;
			} else {
				expectedString = "expected one of " + expectedString;
			}

			return String.Format("Parse error at line {0}, column {1} near \"{2}\": {3}", StartLine, StartColumn, s, expectedString);
		}

		private static string[] BuildExpected(ParseException exception) {
			var list = new List<string>();

			for (int i = 0; i < exception.expectedTokenSequences.Length; i++) {
				for (int j = 0; j < exception.expectedTokenSequences[i].Length; j++) {
					list.Add(exception.tokenImage[exception.expectedTokenSequences[i][j]]);
				}
			}

			return list.ToArray();
		}
	}
}