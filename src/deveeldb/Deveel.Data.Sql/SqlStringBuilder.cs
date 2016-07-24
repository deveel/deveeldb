// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Text;

namespace Deveel.Data.Sql {
	public sealed class SqlStringBuilder {
		private readonly StringBuilder builder;

		public const int DefaultIndentSize = 2;
		public const char DefaultIndentChar = ' ';

		public SqlStringBuilder() {
			builder = new StringBuilder();

			IndentSize = DefaultIndentSize;
			IndentChar = DefaultIndentChar;
		}

		private int IndentCount { get; set; }

		public int IndentSize { get; set; }

		public char IndentChar { get; set; }

		public void Indent() {
			IndentCount += IndentSize;
		}

		public void DeIndent() {
			var count = IndentCount -= IndentSize;
			if (count <= 0)
				count = 0;

			IndentCount = count;
		}

		public char this[int offset] {
			get { return builder[offset]; }
		}

		public void AppendFormat(string format, params object[] args) {
			if (String.IsNullOrEmpty(format))
				throw new ArgumentNullException("format");

			Append(String.Format(format, args));
		}

		public void Append(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			if (lastNewLine) {
				for (int i = 0; i < IndentCount; i++) {
					builder.Append(IndentChar);
				}

				lastNewLine = false;
			}

			builder.Append(s);
		}

		public void Append(object obj) {
			if (obj == null)
				throw new ArgumentNullException("obj");

			Append(obj.ToString());
		}

		public void AppendLine(string s) {
			Append(s);
			AppendLine();
		}

		private bool lastNewLine;

		public void AppendLine() {
			Append(Environment.NewLine);

			lastNewLine = true;
		}

		public override string ToString() {
			return builder.ToString();
		}
	}
}
