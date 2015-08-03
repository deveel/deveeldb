using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	class AlterUserNode : SqlNode, IStatementNode {
		public string UserName { get; private set; }

		public IEnumerable<IAlterUserActionNode> Actions { get; private set; }
	}
}
