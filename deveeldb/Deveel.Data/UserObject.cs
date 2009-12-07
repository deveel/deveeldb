//  
//  ObjectTransfer.cs
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
	/// Represents an instance of an object which is built on
	/// a UDT schema defined.
	/// </summary>
	[Serializable]
	public sealed class UserObject : ICloneable, IEnumerable {
		/// <summary>
		/// Creates a new instance of the object using the
		/// defined <see cref="UserType"/>.
		/// </summary>
		/// <param name="type">The <see cref="UserType"/> instance defining
		/// the attributes of the object.</param>
		public UserObject(UserType type) {
			this.type = type;
			fields = new Hashtable(type.MemberCount);
		}

		/// <summary>
		/// The underlying user-defined type modeling the object.
		/// </summary>
		private readonly UserType type;

		/// <summary>
		/// A dictionary used to handle the values set for each
		/// attribute of the object.
		/// </summary>
		private Hashtable fields;

		/// <summary>
		/// Gets the <see cref="UserType"/> upon which schema the 
		/// object is built.
		/// </summary>
		public UserType Type {
			get { return type; }
		}

		internal ICollection Values {
			get { return fields.Values; }
		}

		public override bool Equals(object obj) {
			UserObject uobj = obj as UserObject;
			if (uobj == null)
				return false;

			if (type != uobj.type)
				return false;

			// since the types are the same, we only need
			// to compare the values
			foreach(DictionaryEntry entry in fields) {
				string fieldName = (string) entry.Key;
				object value = uobj.fields[fieldName];
				object thisValue = entry.Value;

				// if both are null, we proceed to the next
				if (thisValue == null && value == null)
					continue;

				if (thisValue == null || !thisValue.Equals(value))
					return false;
			}

			return true;
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		public override string ToString() {
			return type.ToString();
		}

		public void SetValue(string fieldName, object value) {
			if (fieldName == null || fieldName.Length == 0)
				throw new ArgumentNullException("fieldName");

			UserTypeAttribute attribute = type.FindAttribute(fieldName);
			if (attribute == null)
				throw new ArgumentException("Cannot find the member '" + type.Name + "' into the type.");

			//TODO: check if the value is compatible with the member type...

			fields[fieldName] = value;
		}

		public object GetValue(string fieldName) {
			if (fieldName == null || fieldName.Length == 0)
				throw new ArgumentNullException("fieldName");

			UserTypeAttribute attribute = type.FindAttribute(fieldName);
			if (attribute == null)
				throw new ArgumentException("Cannot find the member '" + type.Name + "' into the type.");

			//TODO: eventually adjust the result value...

			return fields[fieldName];
		}

		#region Implementation of ICloneable

		public object Clone() {
			UserObject ob = new UserObject(type);
			ob.fields = (Hashtable) fields.Clone();
			return ob;
		}

		#endregion

		#region Implementation of IEnumerable

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public IDictionaryEnumerator GetEnumerator() {
			return fields.GetEnumerator();
		}

		#endregion
	}
}