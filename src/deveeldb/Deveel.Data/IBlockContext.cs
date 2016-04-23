// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data {
	/// <summary>
	/// Defines a context that is specific to a <see cref="IBlock"/>
	/// </summary>
	/// <remarks>
	/// The main characteristic of a <see cref="IBlockContext"/> is the
	/// possibility to create a child context, that will inherit the scope
	/// of this context.
	/// </remarks>
	public interface IBlockContext : IContext {
		/// <summary>
		/// Creates a new block context that inherits from this
		/// context the scope.
		/// </summary>
		/// <returns>
		/// Returns a new instance of <see cref="IBlockContext"/> as child
		/// of this context.
		/// </returns>
		IBlockContext CreateBlockContext();
	}
}
