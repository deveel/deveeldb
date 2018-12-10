// 
//  Copyright 2010-2018 Deveel
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

using Deveel.Data.Services;

namespace Deveel.Data {
	/// <summary>
	/// Provides context for a given state of the system
	/// </summary>
	/// <remarks>
	/// Several components of a database system are providing contexts,
	/// to handle configurations, services, variables and a scope.
	/// <para>
	/// The most common context hierarchy is the following:
	/// <list type="bullet">
	///		<listheader>
	///			<term>Context Name</term>
	///			<description></description>
	///     </listheader>
	///		<item>
	///			<term>System</term>
	///			<description>The root level context that all other contexts inherit.</description>
	///		</item>
	///		<item>
	///			<term>Database</term>
	///			<description>The context specific to a single database within a system.</description>
	///		</item>
	///		<item>
	///			<term>Session</term>
	///			<description>The context of a session between the user and the database.</description>
	///		</item>
	///		<item>
	///			<term>Transaction</term>
	///			<description>The context of a single transaction within a session.</description>
	///		</item>
	///		<item>
	///			<term>Query</term>
	///			<description>The context of a single command/command within a transaction.</description>
	///		</item>
	///		<item>
	///			<term>Block</term>
	///         <description>The context of a single execution block within a command execution plan.s</description>
	///		</item>
	/// </list>
	/// </para>
	/// <para>
	/// A context wraps a <see cref="IServiceProvider"/> instance that is disposed at the end of the context.
	/// </para>
	/// </remarks>
	public interface IContext : IDisposable {
		/// <summary>
		/// Gets the parent context of this instance, if any.
		/// </summary>
		/// <remarks>
		/// The only case in which this value is <c>null</c> is when
		/// this is the context of a system, that is the root.
		/// </remarks>
		IContext ParentContext { get; }

		/// <summary>
		/// Gets the name of the context.
		/// </summary>
		/// <remarks>
		/// The name of a context is important for the definition of the wrapped
		/// <see cref="Scope"/>, that is named after this value.
		/// </remarks>
		/// <seealso cref="Scope"/>
		string ContextName { get; }

		/// <summary>
		/// Gets a named scope for this context.
		/// </summary>
		/// <seealso cref="IServiceProvider"/>
		IScope Scope { get; }
	}
}
