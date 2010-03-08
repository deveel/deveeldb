// 
//  Copyright 2010  Deveel
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

using System;

namespace Deveel.Data.Functions {
	/// <summary>
	/// Meta information about a function, used to compile information 
	/// about a particular function.
	/// </summary>
	public interface IFunctionInfo {
		/// <summary>
		/// The name of the function as used by the SQL grammar to reference it.
		/// </summary>
		string Name { get; }

		/// <summary>
		/// The type of function, either Static, Aggregate or StateBased (eg. result
		/// is not dependant entirely from input but from another state for example
		/// RANDOM and UNIQUEKEY functions).
		/// </summary>
		FunctionType Type { get; }

		/// <summary>
		/// The name of the function factory class that this function is handled by.
		/// </summary>
		string FunctionFactoryName { get; }
	}
}