//  
//  UserTypeDef.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data {
	/// <summary>
	/// Describes the metadata of a user-defined type.
	/// </summary>
	public sealed class UserTypeDef {
		/// <summary>
		/// Constructs a <see cref="UserTypeDef"/> having the
		/// given name and deriving from a nother type.
		/// </summary>
		/// <param name="name">The name of the type defined.</param>
		/// <param name="parent">The name of the parent type.</param>
		/// <param name="final">The flag indicating wheter the type can be
		/// inherited or not.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="name"/> of the type is
		/// <c>null</c>.
		/// </exception>
		public UserTypeDef(TableName name, TableName parent, bool final) {
			if (name == null)
				throw new ArgumentNullException("name");

			this.name = name;
			this.parent = parent;
			this.final = final;
			members = new ArrayList();
		}

		/// <summary>
		/// Constructs a <see cref="UserTypeDef"/> having the
		/// given name.
		/// </summary>
		/// <param name="name">The name of the type defined.</param>
		/// <param name="final">The flag indicating wheter the type can be
		/// inherited or not.</param>
		public UserTypeDef(TableName name, bool final)
			: this(name, null, final) {
		}

		/// <summary>
		/// The unique name of the type.
		/// </summary>
		private readonly TableName name;

		/// <summary>
		/// The name of the parent type of this type.
		/// </summary>
		private readonly TableName parent;

		/// <summary>
		/// Whether this type is sealed or not.
		/// </summary>
		private readonly bool final;

		/// <summary>
		/// A list containing all the members of the type.
		/// </summary>
		private ArrayList members;

		private bool immutable;

		public TableName Parent {
			get { return parent; }
		}

		public TableName Name {
			get { return name; }
		}

		public bool IsFinal {
			get { return final; }
		}

		public int MemberCount {
			get { return members.Count; }
		}

		public bool IsReadOnly {
			get { return immutable; }
		}

		public UserTypeMemberDef this[int index] {
			get { return members[index] as UserTypeMemberDef; }
		}

		public UserTypeMemberDef FindMember(string name) {
			for (int i = 0; i < members.Count; i++) {
				UserTypeMemberDef memberDef = (UserTypeMemberDef) members[i];
				if (memberDef.Name == name)
					return memberDef;
			}

			return null;
		}

		public void AddMember(UserTypeMemberDef memberDef) {
			if (immutable)
				throw new InvalidOperationException("This element is not modifiable.");

			if (FindMember(memberDef.Name) != null)
				throw new ArgumentException();

			memberDef.SetOffset(members.Count);
			members.Add(memberDef);
		}

		internal void SetReadOnly() {
			immutable = true;
		}
	}
}