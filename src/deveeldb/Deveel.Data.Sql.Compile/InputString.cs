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

using Antlr4.Runtime;
using Antlr4.Runtime.Tree;

namespace Deveel.Data.Sql.Compile {
	static class InputString {
		public static string AsNotQuoted(string s) {
			if (String.IsNullOrEmpty(s))
				return s;

			if (s[0] == '\'' ||
				s[0] == '\"')
				s = s.Substring(1);
			if (s[s.Length - 1] == '\'' ||
				s[s.Length - 1] == '\"')
				s = s.Substring(0, s.Length - 1);

			return s;
		}

		public static string AsNotQuoted(PlSqlParser.Quoted_stringContext context) {
			return AsNotQuoted(context.GetText());
		}

		public static string AsNotQuoted(IToken token) {
			if (token == null)
				return null;

			return AsNotQuoted(token.Text);
		}

		public static string AsNotQuoted(ITerminalNode node) {
			if (node == null)
				return null;

			return AsNotQuoted(node.GetText());
		}
	}
}
