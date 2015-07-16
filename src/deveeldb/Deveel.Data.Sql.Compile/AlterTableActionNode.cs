using System;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class AlterTableActionNode : SqlNode {
		internal AlterTableActionNode() {
		}

		public string ActionType { get; private set; }

		public DataTypeNode ColumnType { get; private set; }

		public string ColumnName { get; private set; }

		// TODO: Specialized properties
	}
}
