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
using System.Linq;

namespace Deveel.Data.Sql.Parser {
	/// <summary>
	/// Represents a composed name for an object within the system.
	/// </summary>
	/// <remarks>
	/// This node acts as a reference to any object within a database
	/// system, such as <c>TABLE</c>, <c>TRIGGER</c>, <c>COLUMN</c>, etc.
	/// <para>
	/// Like the system object <see cref="ObjectName"/> that this <see cref="ISqlNode"/>
	/// encapsulates, the composition of the object is complex and can be formed by
	/// multiple parts (eg. <c>schema.table.column</c>, <c>schema.table</c>, <c>table.*</c>).
	/// </para>
	/// </remarks>
	class ObjectNameNode : SqlNode {
		internal ObjectNameNode() {
		}

		/// <summary>
		/// The full object name as composed from the input SQL string analyzed.
		/// </summary>
		/// <seealso cref="ObjectName"/>
		public string Name { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName.Equals("name_part")) {
				node = node.ChildNodes.FirstOrDefault();
			}

			string text;
			if (node is IdentifierNode) {
				var idNode = (IdentifierNode) node;
				text = idNode.Text;
			} else if (node is SqlKeyNode) {
				text = ((SqlKeyNode) node).Text;

				if (!String.Equals(text, "*"))
					throw new SqlParseException(String.Format("Invalid object name part '{0}' provided.", text));
			} else {
				throw new SqlParseException(String.Format("The node of type '{0}' is not allowed.", node.GetType()));
			}

			if (Name != null) {
				Name = String.Format("{0}.{1}", Name, text);
			} else {
				Name = text;
			}

			return base.OnChildNode(node);
		}
	}
}