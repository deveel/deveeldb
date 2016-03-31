using System;

namespace Deveel.Data.Sql.Tables {
	static class ForeignKeyActionExtensions {
		public static string AsSqlString(this ForeignKeyAction action) {
			if (action == ForeignKeyAction.SetNull)
				return "SET NULL";
			if (action == ForeignKeyAction.SetDefault)
				return "SET DEFAULT";
			if (action == ForeignKeyAction.NoAction)
				return "NO ACTION";

			return "CASCADE";
		}
	}
}
