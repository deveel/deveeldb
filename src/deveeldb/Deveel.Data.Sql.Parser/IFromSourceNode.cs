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
	/// Defines the base contract of the source of a query.
	/// </summary>
	/// <remarks>
	/// This can only be in the form of <see cref="FromTableSourceNode"/> or
	/// <see cref="FromQuerySourceNode"/>, that means only a table or
	/// a sub-query can be source of query.
	/// </remarks>
	public interface IFromSourceNode : ISqlNode {
		/// <summary>
		/// Gets an alias that uniquely identifies the source within
		/// a query context.
		/// </summary>
		/// <remarks>
		/// This value can be <c>null</c> that means the name will be
		/// resolved by its main component.
		/// </remarks>
		IdentifierNode Alias { get; }
	}
}