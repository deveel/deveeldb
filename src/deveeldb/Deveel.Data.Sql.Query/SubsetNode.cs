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

using Deveel.Data;

namespace Deveel.Data.Sql.Query {
	class SubsetNode : SingleQueryPlanNode {
		public SubsetNode(IQueryPlanNode child, ObjectName[] originalColumnNames, ObjectName[] aliasColumnNames) 
			: base(child) {
			OriginalColumnNames = originalColumnNames;
			AliasColumnNames = aliasColumnNames;
		}

		public ObjectName[] OriginalColumnNames { get; private set; }

		public ObjectName[] AliasColumnNames { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var table = Child.Evaluate(context);
			return table.Subset(OriginalColumnNames, AliasColumnNames);
		}

		public void SetAliasParentName(ObjectName parentName) {
			var aliases = new ObjectName[AliasColumnNames.Length];
			for (int i = 0; i < aliases.Length; i++) {
				aliases[i] = new ObjectName(parentName, aliases[i].Name);
			}

			AliasColumnNames = aliases;
		}
	}
}
