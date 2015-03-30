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

using Deveel.Data.Caching;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.DbSystem {
	//TODO: Add many more functions ... this is a sort of placeholder for the moment
	/// <summary>
	/// Provides a context for executing queries, accessing the
	/// system resources and evaluation context.
	/// </summary>
	public interface IQueryContext : IDisposable {
		IUserSession Session { get; }

		/// <summary>
		/// Gets the system context parent of this context.
		/// </summary>
		ISystemContext SystemContext { get; }

		ICache TableCache { get; }


		/// <summary>
		/// Gets a value that indicates if the current context is in
		/// an exception state or not.
		/// </summary>
		/// <seealso cref="SetExceptionState"/>
		bool IsExceptionState { get; }

		IRoutineResolver RoutineResolver { get; }

		/// <summary>
		/// Computes a new random number, that is ensured to be unique 
		/// within the execution context.
		/// </summary>
		/// <param name="bitSize">The number of bits the final random number must 
		/// have. This number can only be 2, 4, 8 or 16.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents a unique random number
		/// computed within this execution context.
		/// </returns>
		SqlNumber NextRandom(int bitSize);

		/// <summary>
		/// Marks the execution context as in an exception state.
		/// </summary>
		/// <param name="exception">The exception that causes the change of
		/// state of the context.</param>
		/// <seealso cref="IsExceptionState"/>
		/// <seealso cref="GetException"/>
		void SetExceptionState(Exception exception);

		/// <summary>
		/// If this context is in an exception state, this method
		/// gets the exception that caused the change of state.
		/// </summary>
		/// <returns>
		/// Returns an <see cref="Exception"/> that is the origin
		/// of the context change state, or <b>null</b> if the context
		/// is not in an exception state.
		/// </returns>
		/// <seealso cref="IsExceptionState"/>
		/// <seealso cref="SetExceptionState"/>
		Exception GetException();

		/// <summary>
		/// Gets a database object that has the name given.
		/// </summary>
		/// <param name="objName">The fully qualified name of the object to
		/// find in the given context.</param>
		/// <returns>
		/// Returns an instance of a <see cref="IDbObject"/> identified
		/// by the given fully qualified name, or <c>null</c> if none object
		/// was found having that name.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="objName">name paramete</paramref> passed
		/// is <c>null</c>.
		/// </exception>
		/// <seealso cref="IDbObject"/>
		IDbObject GetObject(ObjectName objName);
	}
}