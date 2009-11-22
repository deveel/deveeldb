using System;

using Deveel.Data.Commands;
using Deveel.Data.DbModel;

namespace Deveel.Data.Metadata {
	public abstract class GenerateStatementCommandBase : Command {
		private ISqlStatementFormatter sqlFormatter;

		protected ISqlStatementFormatter Formatter {
			get {
				if (sqlFormatter == null)
					sqlFormatter = (ISqlStatementFormatter) Services.Resolve(typeof(ISqlStatementFormatter));
				return sqlFormatter;
			}
		}

		protected string TrimTrailingComma(string sql) {
			if (sql != null && sql.TrimEnd().EndsWith(",")) {
				string tmp = sql.TrimEnd();
				return tmp.Substring(0, tmp.Length - 1);
			}
			return sql;
		}
	}
}