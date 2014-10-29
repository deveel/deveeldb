// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Compile {
	[Serializable]
	public sealed class SqlConstantExpressionNode : SqlNode, IExpressionNode {
		public ISqlObject Value { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SqlKeyNode) {
				var keyNode = (SqlKeyNode) node;
				if (String.Equals(keyNode.Text, "true", StringComparison.OrdinalIgnoreCase)) {
					Value = SqlBoolean.True;
				} else if (String.Equals(keyNode.Text, "false", StringComparison.OrdinalIgnoreCase)) {
					Value = SqlBoolean.False;
				} else if (String.Equals(keyNode.Text, "null", StringComparison.OrdinalIgnoreCase)) {
					Value = SqlNull.Value;
				} else {
					Value = SqlString.Unicode(((SqlKeyNode) node).Text);
				}
			} else if (node is IntegerLiteralNode) {
				Value = new SqlNumber(((IntegerLiteralNode) node).BigValue);
			} else if (node is NumberLiteralNode) {
				Value = new SqlNumber(((NumberLiteralNode) node).BigValue);
			} else if (node is StringLiteralNode) {
				Value = SqlString.Unicode(((StringLiteralNode) node).Value);
			}

			return base.OnChildNode(node);
		}
	}
}