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
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Deveel.Data.DbSystem {
	public static class QueryContextExtensions {
		public static User User(this IQueryContext context) {
			return context.Session.User;
		}

		public static IDatabase Database(this IQueryContext context) {
			return context.Session.Database;
		}

		public static ITable GetTable(this IQueryContext context, ObjectName tableName) {
			var obj = context.GetObject(tableName);
			if (obj == null)
				return null;

			if (obj.ObjectType != DbObjectType.Table)
				throw new InvalidOperationException(String.Format("The object '{0}' is {1} (expecting a table)", obj.FullName, obj.ObjectType));

			return (ITable) obj;
		}

		internal static IDbTable GetDbTable(this IQueryContext context, ObjectName tableName) {
			return (IDbTable) context.GetTable(tableName);
		}

		public static ISequence GetSequence(this IQueryContext context, ObjectName sequenceName) {
			var obj = context.GetObject(sequenceName);
			if (obj == null)
				return null;

			if (obj.ObjectType != DbObjectType.Sequence)
				throw new InvalidOperationException(String.Format("The object '{0}' is {1} (expecting a sequence)", obj.FullName, obj.ObjectType));

			return (ISequence) obj;
		}

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
		public static SqlNumber GetNextValue(this IQueryContext context, ObjectName sequenceName) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			return sequence.NextValue();
		}

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
		public static SqlNumber GetCurrentValue(this IQueryContext context, ObjectName sequenceName) {
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
		public static void SetCurrentValue(this IQueryContext context, ObjectName sequenceName, SqlNumber value) {
			var sequence = context.GetSequence(sequenceName);
			if (sequence == null)
				throw new InvalidOperationException(String.Format("Sequence {0} was not found.", sequenceName));

			sequence.SetValue(value);
		}
	}
}
