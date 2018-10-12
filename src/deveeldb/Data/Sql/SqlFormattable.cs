using System;

namespace Deveel.Data.Sql {
	public static class SqlFormattable {
		public static string ToSqlString(this ISqlFormattable obj) {
			var builder = new SqlStringBuilder();
			obj.AppendTo(builder);

			return builder.ToString();
		}
	}
}