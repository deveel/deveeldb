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
	/// Provides a context for executing queries, accessing the
	/// system resources and evaluation context.
	/// </summary>
	/// <remarks>
	/// <para>
	/// Query contexts are created at the creation of <see cref="IQuery"/>
	/// and are contained by them, to be disposed at the disposal
	/// of the containing <see cref="IQuery"/>.
	/// </para>
	/// <para>
	/// Instances of this object inherit the context state of the parent
	/// <see cref="ISessionContext"/>
	/// </para>
	/// <para>
	/// As a <see cref="IBlockContext"/> this object passes its state 
	/// to the <see cref="IBlockContext"/> instances created.
	/// </para>
	/// </remarks>
	/// <seealso cref="ISessionContext"/>
	public interface IQueryContext : IBlockContext {
		/// <summary>
		/// Gets the context of the session context parent of the
		/// <see cref="IQuery"/> that encapsulates this context.
		/// </summary>
        ISessionContext SessionContext { get; }
	}
}