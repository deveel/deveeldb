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

namespace Deveel.Data.Sql.Parser {
	class PlSqlBlockNode : SqlNode {
		public string Label { get; private set; }

		public IEnumerable<IDeclareNode> Declarations { get; private set; }

		public IEnumerable<IStatementNode> Statements { get; private set; }

		protected override void OnNodeInit() {
			var label = this.FindNode<LabelNode>();
			if (label != null)
				Label = label.Text;

			Declarations = this.FindNodes<IDeclareNode>();
			Statements = this.FindNodes<IStatementNode>();

			base.OnNodeInit();
		}
	}
}
