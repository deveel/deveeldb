using System;

namespace Deveel.Data.Sql.Constraints {
	public sealed class ColumnConstraintInfo : ConstraintInfo {
		public ColumnConstraintInfo(ObjectName tableName, string constraintName, string[] columns) 
			: base(tableName, constraintName) {
		}
	}
}