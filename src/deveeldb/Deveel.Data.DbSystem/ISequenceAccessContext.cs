// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// Defines the functionalities needed to access sequences within
	/// a database system context.
	/// </summary>
	public interface ISequenceAccessContext : IDisposable {
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
	}
}