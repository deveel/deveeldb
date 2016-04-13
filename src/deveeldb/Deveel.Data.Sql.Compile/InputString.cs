using System;

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
	}
}
