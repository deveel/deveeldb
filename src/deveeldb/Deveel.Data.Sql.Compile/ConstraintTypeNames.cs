using System;

namespace Deveel.Data.Sql.Compile {
	static class ConstraintTypeNames {
		public const string Check = "CHECK";
		public const string ForeignKey = "FKEY";
		public const string UniqueKey = "UNIQUE";
		public const string PrimaryKey = "PKEY";
	}
}
