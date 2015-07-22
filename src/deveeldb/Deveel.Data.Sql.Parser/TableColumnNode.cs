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

namespace Deveel.Data.Sql.Parser {
	class TableColumnNode : SqlNode, ITableElementNode {
		internal TableColumnNode() {
		}

		public IdentifierNode ColumnName { get; private set; }

		public DataTypeNode DataType { get; private set; }

		public IEnumerable<ColumnConstraintNode> Constraints { get; private set; }

		public IExpressionNode Default { get; private set; }

		public bool IsIdentity { get; private set; }

		protected override void OnNodeInit() {
			ColumnName = this.FindNode<IdentifierNode>();
			DataType = this.FindNode<DataTypeNode>();
			Default = this.FindNode<IExpressionNode>();
			IsIdentity = this.HasOptionalNode("column_identity_opt");

			Constraints = this.FindNodes<ColumnConstraintNode>();
		}
	}
}
