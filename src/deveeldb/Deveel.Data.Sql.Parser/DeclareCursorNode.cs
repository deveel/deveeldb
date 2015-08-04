using System;

namespace Deveel.Data.Sql.Parser {
	class DeclareCursorNode : SqlNode, IDeclareNode {
		public string CursorName { get; private set; }
	}
}
