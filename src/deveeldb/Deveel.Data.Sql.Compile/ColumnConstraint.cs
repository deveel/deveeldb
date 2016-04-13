using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Compile {
	class ColumnConstraint {
		public string ColumnName { get; set; }

		public ConstraintType Type { get; set; }
	}
}