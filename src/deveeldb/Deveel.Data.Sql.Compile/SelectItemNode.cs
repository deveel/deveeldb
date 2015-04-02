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

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// A single item selected within a query node tree.
	/// </summary>
	[Serializable]
	class SelectItemNode : SqlNode {
		/// <summary>
		/// Gets the name of the item selected, if the kind of item 
		/// selected is an object name (variable, column, etc.).
		/// </summary>
		public ObjectNameNode Name { get; private set; }

		/// <summary>
		/// Gets an expression to be returned in the result of the
		/// selection, if the item is set to be an expression.
		/// </summary>
		public IExpressionNode Expression { get; private set; }

		/// <summary>
		/// Gets an optional name that will uniquely identify the 
		/// selected item within the query context.
		/// </summary>
		public IdentifierNode Alias { get; private set; }

		protected override void OnNodeInit() {
			Expression = ChildNodes.OfType<IExpressionNode>().FirstOrDefault();
			Name = ChildNodes.OfType<ObjectNameNode>().FirstOrDefault();

			var aliasNode = this.FindByName("select_as_opt");
			if (aliasNode != null)
				Alias = aliasNode.FindNode<IdentifierNode>();
		}
	}
}