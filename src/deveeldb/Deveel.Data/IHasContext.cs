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
	/// Marks an element of the system as handler of a context
	/// </summary>
	/// <remarks>
	/// Implementations of this contract provide access to a
	/// <see cref="IContext"/> object, that handles the contextual
	/// references to configurations and services.
	/// </remarks>
	/// <seealso cref="IContext"/>
	public interface IHasContext {
		/// <summary>
		/// Gets the context handled by the object.
		/// </summary>
		IContext Context { get; }
	}
}
