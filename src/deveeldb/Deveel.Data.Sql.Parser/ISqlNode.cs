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
	/// <summary>
	/// Defines the contract for nodes in an AST model for a SQL
	/// grammar analysis and parsing.
	/// </summary>
	public interface ISqlNode {
		/// <summary>
		/// Gets the name of the node analyzed from the parser.
		/// </summary>
		string NodeName { get; }

		/// <summary>
		/// Gets a reference to the parent <see cref="ISqlNode"/>, if any.
		/// </summary>
		ISqlNode Parent { get; }

		/// <summary>
		/// Gets a read-only enumeration of the children nodes, if any.
		/// </summary>
		IEnumerable<ISqlNode> ChildNodes { get; }
			
		/// <summary>
		/// Gets an enumeration of the tokens composing the this node.
		/// </summary>
		/// <seealso cref="Token"/>
		IEnumerable<Token> Tokens { get; }

		int Line { get; }

		int Column { get; }
	}
}