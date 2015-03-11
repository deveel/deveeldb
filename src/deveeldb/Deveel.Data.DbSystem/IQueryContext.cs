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
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.DbSystem {
	//TODO: Add many more functions ... this is a sort of placeholder for the moment
	/// <summary>
	/// Provides a context for executing queries, accessing the
	/// system resources and evaluation context.
	/// </summary>
	public interface IQueryContext : IDisposable {
		User User { get; }

		ISystemContext SystemContext { get; }

		ICache TableCache { get; }

		IQueryPlanContext QueryPlanContext { get; }


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
		/// Increments the sequence and returns the computed value.
		/// </summary>
		/// <param name="sequenceName">The name of the sequence to increment and
		/// whose incremented value must be returned.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the result of
		/// the increment operation over the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		SqlNumber GetNextValue(ObjectName sequenceName);

		/// <summary>
		/// Gets the current value of the sequence.
		/// </summary>
		/// <param name="sequenceName">The name of the sequence whose current value
		/// must be obtained.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the current value
		/// of the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		SqlNumber GetCurrentValue(ObjectName sequenceName);

		/// <summary>
		/// Sets the current value of the sequence, overriding the increment
		/// mechanism in place.
		/// </summary>
		/// <param name="sequenceName">The name of the sequence whose current state
		/// to be set.</param>
		/// <param name="value">The numeric value to set.</param>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		void SetCurrentValue(ObjectName sequenceName, SqlNumber value);

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
	}
}