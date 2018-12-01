using System;
using System.Collections.Generic;
using System.Text;

using Antlr4.Runtime.Misc;

namespace Deveel.Data.Sql.Parsing {
	static partial class SqlParseUtil {
		public static int? PositiveInteger(PlSqlParser.NumericContext context) {
			if (context == null)
				return null;

			var text = context.GetText();

			if (!Int32.TryParse(text, out var value))
				throw new ParseCanceledException($"Numeric '{text}' is not an integer.");
			if (value < 0)
				throw new ParseCanceledException($"Integer '{text}' is not positive.");

			return value;
		}
	}
}
