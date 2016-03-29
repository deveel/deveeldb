// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Sql.Parser {
	class CurrentTimeFunctionNode : SqlNode, IExpressionNode {
		public string FunctionName { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode) {
				var key = (SqlKeyNode) node;
				if (String.Equals(key.Text, "CURRENT_TIME", StringComparison.OrdinalIgnoreCase)) {
					FunctionName = "TIME";
				} else if (String.Equals(key.Text, "CURRENT_TIMESTAMP", StringComparison.OrdinalIgnoreCase)) {
					FunctionName = "TIMESTAMP";
				} else if (String.Equals(key.Text, "CURRENT_DATE", StringComparison.OrdinalIgnoreCase)) {
					FunctionName = "DATE";
				} else {
					throw Error(String.Format("The keyword '{0}' is not allowed.", key.Text));
				}
			}

			return base.OnChildNode(node);
		}
	}
}
