using System;

namespace Deveel.Data.Sql.Parser {
	public sealed class InsertStatementNode : SqlNode, IStatementNode {
		internal InsertStatementNode() {
		}

		public ValuesInsertNode ValuesInsert { get; private set; }
	}
}
