// 
//  Copyright 2010  Deveel
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