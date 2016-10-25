using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements.Build {
	public sealed class ColumnConstraintInfo {
		public ColumnConstraintInfo(ConstraintType constraintType) {
			if (constraintType == ConstraintType.Check)
				throw new ArgumentException("Check is not a column-level constraint");

			ConstraintType = constraintType;
		}

		public ConstraintType ConstraintType { get; private set; }

		public ObjectName ReferencedTable { get; set; }

		public string ReferencedColumnName { get; set; }

		public ForeignKeyAction ActionOnDelete { get; set; }

		public ForeignKeyAction ActionOnUpdate { get; set; }
	}
}
