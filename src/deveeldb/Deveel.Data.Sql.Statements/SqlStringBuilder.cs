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

namespace Deveel.Data.Sql.Statements {
	public sealed class SqlStringBuilder {
		private readonly StringBuilder builder;

		internal SqlStringBuilder() {
			builder = new StringBuilder();
		}

		private int IndentCount { get; set; }

		public void Indent() {
			IndentCount++;
		}

		public void DeIndent() {
			var count = IndentCount--;
			if (count <= 0)
				count = 0;

			IndentCount = count;
		}

		public void Append(string format, params object[] args) {
			if (String.IsNullOrEmpty(format))
				throw new ArgumentNullException("format");

			Append(String.Format(format, args));
		}

		public void Append(string s) {
			if (String.IsNullOrEmpty(s))
				throw new ArgumentNullException("s");

			for (int i = 0; i < IndentCount; i++) {
				builder.Append(" ");
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

		public void AppendLine() {
			Append('\n');
		}

		public override string ToString() {
			return builder.ToString();
		}
	}
}
