using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Parser {
	class GrantRoleStatementNode : SqlNode {
		public IEnumerable<string> Grantees { get; private set; }
				
		public	 IEnumerable<string> Roles { get; private set; }
		
		public	bool WithAdmin { get; private set; }
	}
}
