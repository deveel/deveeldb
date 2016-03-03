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

namespace Deveel.Data.Sql.Parser {
	class LimitNode : SqlNode {
		public long Count { get; private set; }

		public long Offset { get; private set; }

		protected override void OnNodeInit() {
			long? arg1 = null, arg2 = null;
			foreach (var childNode in ChildNodes) {
				if (childNode is IntegerLiteralNode) {
					if (arg1 == null) {
						arg1 = ((IntegerLiteralNode) childNode).Value;
					} else if (arg2 == null) {
						arg2 = ((IntegerLiteralNode) childNode).Value;
					}
				}
			}

			if (arg1 == null && arg2 == null)
				throw new SqlParseException("At least one parameter is required in a LIMIT.");

			if (arg2 != null) {
				Offset = arg1.Value;
				Count = arg2.Value;
			} else {
				Count = arg1.Value;
			}

			base.OnNodeInit();
		}
	}
}
