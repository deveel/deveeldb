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

namespace Deveel.Data.Sql.Compile {
	/// <summary>
	/// The node in an SQL query that defines the sources from which to
	/// retrieve the data queried.
	/// </summary>
	/// <seealso cref="IFromSourceNode"/>
	[Serializable]
	class FromClauseNode : SqlNode {
		/// <summary>
		/// Gets a read-only list of sources for the query.
		/// </summary>
		/// <seealso cref="IFromSourceNode"/>
		public IEnumerable<IFromSourceNode> Sources { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node.NodeName == "from_source_list") {
				GetSources(node);
			}

			return base.OnChildNode(node);
		}

		private void GetSources(ISqlNode node) {
			var sources = new List<IFromSourceNode>();

			foreach (var childNode in node.ChildNodes) {
				if (childNode is IFromSourceNode) {
					sources.Add((IFromSourceNode)childNode);
				}
			}

			Sources = sources.AsReadOnly();
		}
	}
}