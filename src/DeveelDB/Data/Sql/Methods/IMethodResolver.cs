// 
//  Copyright 2010-2017 Deveel
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

namespace Deveel.Data.Sql.Methods {
	/// <summary>
	/// Provides an interface for resolving methods defined in a system
	/// </summary>
	public interface IMethodResolver {
		/// <summary>
		/// Attempts to resolve a method defined in the underlying system
		/// from the specified invoke information.
		/// </summary>
		/// <param name="context">The context used to resolve the method</param>
		/// <param name="invoke">The information of the invocation to the method
		/// (the name of the method and the arguments).</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlMethod"/> that corresponds to the
		/// given information in the context given, or <c>null</c> if it was not
		/// possible to resovle any method for the provided information.
		/// </returns>
		SqlMethod ResolveMethod(IContext context, Invoke invoke);
	}
}