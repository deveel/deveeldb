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

namespace Deveel.Data.DbSystem {
	public static class UserSessionExtensions {
		#region Sequences

		/// <summary>
		/// Requests the sequence generator for the next value.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="name"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <returns></returns>
		public static SqlNumber NextSequenceValue(this IUserSession session, string name) {
			// Resolve and ambiguity test
			var seqName = session.ResolveObjectName(name);
			return session.Transaction.NextValue(seqName);
		}

		/// <summary>
		/// Returns the current sequence value for the given sequence generator.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="name"></param>
		/// <remarks>
		/// The value returned is the same value returned by <see cref="NextSequenceValue"/>.
		/// <para>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If no value was returned by <see cref="NextSequenceValue"/>.
		/// </exception>
		public static SqlNumber LastSequenceValue(this IUserSession session, string name) {
			// Resolve and ambiguity test
			var seqName = session.ResolveObjectName(name);
			return session.Transaction.LastValue(seqName);
		}

		/// <summary>
		/// Sets the sequence value for the given sequence generator.
		/// </summary>
		/// <param name="session"></param>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the generator does not exist or it is not possible to set the 
		/// value for the generator.
		/// </exception>
		public static void SetSequenceValue(this IUserSession session, string name, SqlNumber value) {
			// Resolve and ambiguity test
			var seqName = session.ResolveObjectName(name);
			session.Transaction.SetValue(seqName, value);
		}

		#endregion
	}
}