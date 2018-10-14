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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Security {
	/// <summary>
	/// Describes a type of privilege that can be granted
	/// on a database object
	/// </summary>
	/// <remarks>
	/// <para>
	/// This object allows the dynamic definition of privileges:
	/// implementations of the system can define additional
	/// privileges to the one natively provided
	/// </para>
	/// <para>
	/// The base information for the management of privileges is
	/// a numeric value that uniquely identifies through the system
	/// </para>
	/// <para>
	/// <b>Note:</b> the numeric value of a privilege must be unique
	/// and be an exponent of 2 (to be bitwise compatible)
	/// </para>
	/// </remarks>
	public struct Privilege {
		private readonly int value;

		/// <summary>
		/// Constructs a new <see cref="Privilege"/> with a given
		/// numeric value
		/// </summary>
		/// <param name="value">The numeric value of the privilege</param>
		public Privilege(int value) {
			this.value = value;
		}

		/// <summary>
		/// An empty privilege that permits no operations
		/// </summary>
		public static Privilege None = new Privilege(0);

		/// <summary>
		/// Gets a boolean value indicating if this privilege is empty
		/// </summary>
		public bool IsNone => value == 0;

		/// <summary>
		/// Adds a privilege to this one
		/// </summary>
		/// <param name="privilege">The privilege to add</param>
		/// <returns>
		/// Returns a new instance of <see cref="Privilege"/> that
		/// is the result of the addition of this privilege with
		/// the specified one
		/// </returns>
		public Privilege Add(Privilege privilege) {
			return new Privilege(value | privilege.value);
		}

		/// <summary>
		/// Removes the given privilege from this one
		/// </summary>
		/// <param name="privilege">The </param>
		/// <returns></returns>
		public Privilege Remove(Privilege privilege) {
			int andPriv = (value & privilege.value);
			return new Privilege(value ^ andPriv);
		}

		public bool Permits(Privilege privilege) {
			return (value & privilege.value) != 0;
		}

		public Privilege Next() {
			return new Privilege(value ^ 2);
		}

		public string ToString(IContext context) {
			var resolvers = context.GetServices<IPrivilegeResolver>();
			return ToString(resolvers.ToArray());
		}

		public string ToString(IPrivilegeResolver[] resolvers) {
			var result = new List<string>();

			foreach (var resolver in resolvers) {
				var res1 = resolver.ToString(this);
				result.AddRange(res1);
			}

			return String.Join(", ", result);
		}

		public override string ToString() {
			return ToString(new[] {SqlPrivileges.Resolver});
		}

		#region Operators

		public static Privilege operator +(Privilege a, Privilege b) {
			return a.Add(b);
		}

		public static Privilege operator -(Privilege a, Privilege b) {
			return a.Remove(b);
		}

		#endregion
	}
}