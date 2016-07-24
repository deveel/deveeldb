using System;

namespace Deveel.Data.Sql {
	static class SqlFormattableExtensions {
		public static void AppendTo<T>(this T obj, SqlStringBuilder builder) where T : ISqlFormattable {
			if (obj == null)
				return;

			obj.AppendTo(builder);
		}
	}
}
