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

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class CreateTableNode : SqlNode, IStatementNode {
		internal CreateTableNode() {
		}

		public ObjectNameNode TableName { get; internal set; }

		public bool IfNotExists { get; internal set; }

		public bool Temporary { get; internal set; }

		public IEnumerable<TableColumnNode> Columns { get; internal set; }

		public IEnumerable<TableConstraintNode> Constraints { get; internal set; }

		protected override void OnNodeInit() {
			TableName = this.FindNode<ObjectNameNode>();
			IfNotExists = this.HasOptionalNode("if_not_exists_opt");
			Temporary = this.HasOptionalNode("temporary_opt");

			var elements = this.FindNodes<ITableElementNode>().ToList();
			Columns = elements.OfType<TableColumnNode>();
			Constraints = elements.OfType<TableConstraintNode>();
		}
	}
}
