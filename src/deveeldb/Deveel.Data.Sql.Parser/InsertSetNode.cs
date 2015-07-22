using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	class InsertSetNode : SqlNode {
		internal InsertSetNode() {
		}

		public IEnumerable<IExpressionNode> Values { get; private set; }
	}
}
