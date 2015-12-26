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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// An node that represents a constant value set within a context
	/// of an SQL command.
	/// </summary>
	class SqlConstantExpressionNode : SqlNode, IExpressionNode {
		internal SqlConstantExpressionNode() {
		}

		/// <summary>
		/// Gets an immutable instance of <see cref="Objects.ISqlObject"/> that represents the
		/// constant value.
		/// </summary>
		public Objects.ISqlObject Value { get; private set; }

		/// <inheritdoc/>
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
					Value = new SqlString(((SqlKeyNode) node).Text.ToCharArray());
				}
			} else if (node is IntegerLiteralNode) {
				Value = new SqlNumber(((IntegerLiteralNode) node).BigValue);
			} else if (node is NumberLiteralNode) {
				Value = new SqlNumber(((NumberLiteralNode) node).BigValue);
			} else if (node is StringLiteralNode) {
				Value = new SqlString(((StringLiteralNode) node).Value.ToCharArray());
			}

			return base.OnChildNode(node);
		}
	}
}