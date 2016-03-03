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
	/// An implementation of <see cref="ISqlNode"/> that accepts
	/// visits from a <see cref="ISqlNodeVisitor"/>
	/// </summary>
	public interface ISqlVisitableNode : ISqlNode {
		/// <summary>
		/// Called by an implementation of <see cref="ISqlNodeVisitor"/>
		/// during the tree-node evaluation.
		/// </summary>
		/// <param name="visitor">The visitor object that calls this method.</param>
		void Accept(ISqlNodeVisitor visitor);
	}
}