using System;

using Antlr4.Runtime.Misc;

namespace Deveel.Data.Sql.Compile {
	class Number {
		public static int? PositiveInteger(PlSqlParser.NumericContext context) {
			if (context == null)
				return null;

			var text = context.GetText();
			int value;
			if (!Int32.TryParse(text, out value))
				throw new ParseCanceledException(String.Format("Numeric '{0}' is not a positive integer.", text));

			return value;
		}
	}
}
