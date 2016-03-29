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

namespace Deveel.Data.Sql.Objects {
	public static class SqlStringExtensions {
		// Statics for the tokens.
		private const char ZeroOrMoreChars = '%';
		private const char OneChar = '_';

		private static bool IsWildCard(char ch) {
			return (ch == OneChar || ch == ZeroOrMoreChars);
		}

		public static SqlBoolean PatternMatch(this ISqlString pattern, string expression, char escapeChar) {
			throw new NotImplementedException();
		}

		public static SqlString Substring(this ISqlString source, int offset) {
			return Substring(source, offset, (int)source.Length - offset);
		}

		public static SqlString Substring(this ISqlString source, int offset, int count) {
			if (source == null || source.IsNull)
				return SqlString.Null;

			var en = source.GetEnumerator();
			var sb = new StringBuilder(count);

			int index = -1;
			while (en.MoveNext()) {
				if (++index < offset)
					continue;

				sb.Append(en.Current);

				if (index == count - 1)
					break;
			}

#if PCL
			var s = sb.ToString();
			return new SqlString(s);
#else
			var chars = new char[count];
			sb.CopyTo(0, chars, 0, count);
			return new SqlString(chars);
#endif
		}

		public static SqlNumber IndexOf(this ISqlString pattern, SqlString expression) {
			// TODO: Implement a version of the Boyer-Moore algorithm over a SQL String
			throw new NotImplementedException();
		}
	}
}