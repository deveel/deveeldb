// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class DropTableStatementNode : SqlStatementNode {
		public IEnumerable<string> TableNames { get; private set; }

		public bool IfExists { get; private set; }

		protected override void OnNodeInit() {
			var tableNames = this.FindNodes<ObjectNameNode>();
			TableNames = tableNames.Select(x => x.Name);

			var ifExistsOpt = this.FindByName("if_exists_opt");
			if (ifExistsOpt != null && ifExistsOpt.ChildNodes.Any())
				IfExists = true;

			base.OnNodeInit();
		}

		protected override void BuildStatement(StatementBuilder builder) {
			builder.Statements.Add(new DropTableStatement(TableNames.ToArray(), IfExists));
		}
	}
}
