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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Sequences {
	public static class QueryExtensions {
		public static ISequence GetSequence(this IQuery context, ObjectName sequenceName) {
			return context.GetObject(DbObjectType.Sequence, sequenceName, AccessType.Read) as ISequence;
		}

		/// <summary>
		/// Increments the sequence and returns the computed value.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="sequenceName">The name of the sequence to increment and
		/// whose incremented value must be returned.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the result of
		/// the increment operation over the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public static SqlNumber GetNextValue(this IQuery context, ObjectName sequenceName) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			return sequence.NextValue();
		}

		/// <summary>
		/// Gets the current value of the sequence.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="sequenceName">The name of the sequence whose current value
		/// must be obtained.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents the current value
		/// of the sequence identified by the given name.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If none sequence was found for the given <paramref name="sequenceName"/>.
		/// </exception>
		public static SqlNumber GetCurrentValue(this IQuery context, ObjectName sequenceName) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			return sequence.GetCurrentValue();
		}

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
		public static void SetCurrentValue(this IQuery context, ObjectName sequenceName, SqlNumber value) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			sequence.SetValue(value);
		}
	}
}
