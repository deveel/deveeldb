using System;

namespace Deveel.Data.Sql.Compile {
	class IntoClause {
		public string[] Variables { get; set; }

		public ObjectName TableName { get; set; }
	}
}
