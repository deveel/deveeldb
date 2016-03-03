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
	/// <summary>
	/// An expression node that references an object within the database
	/// context (such as a table, a type, a variable, etc.).
	/// </summary>
	class SqlReferenceExpressionNode : SqlNode, IExpressionNode {
		internal SqlReferenceExpressionNode() {
		}

		/// <summary>
		/// Gets the full name of the object references.
		/// </summary>
		public ObjectNameNode Reference { get; private set; }

		/// <inheritdoc/>
		protected override ISqlNode OnChildNode(ISqlNode node) {
			Reference = (ObjectNameNode) node;
			return base.OnChildNode(node);
		}
	}
}