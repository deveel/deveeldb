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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Routines {
	/// <summary>
	/// The system uses instances of this interface to resolve
	/// routines given a user invocation.
	/// </summary>
	public interface IRoutineResolver {
		/// <summary>
		/// Resolves a routine that matches the given invocation
		/// within the context provided.
		/// </summary>
		/// <param name="request">The routine invocation request used to resolve
		/// the routine.</param>
		/// <param name="context">The parent query context.</param>
		/// <returns>
		/// Returns an instance of <see cref="IRoutine"/> that matches the
		/// given request, or <c>null</c> if no routine was found in the
		/// underlying context.
		/// </returns>
		IRoutine ResolveRoutine(Invoke request, IQueryContext context);
	}
}