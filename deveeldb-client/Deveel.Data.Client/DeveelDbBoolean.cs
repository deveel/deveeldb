// 
//  DeveelDbBoolean.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data.SqlTypes;

namespace Deveel.Data.Client {
	/// <summary>
	/// The definition of a boolean type within a DeveelDB
	/// database engine and returned to the client.
	/// </summary>
	public struct DeveelDbBoolean : IComparable, INullable {
		/// <summary>
		/// Constructs a new <see cref="DeveelDbBoolean"/> object
		/// specifying the value and a flag indicating if this is
		/// a null.
		/// </summary>
		/// <param name="value">The native boolean value of this type.</param>
		/// <param name="isNull">Whether the boolean value is intended as
		/// <c>null</c>.</param>
		private DeveelDbBoolean(bool value, bool isNull) {
			this.value = value;
			is_null = isNull;
		}

		/// <summary>
		/// Constructs a new <see cref="DeveelDbBoolean"/> object
		/// specifying a non-null value.
		/// </summary>
		/// <param name="value">The native boolean value of this type.</param>
		public DeveelDbBoolean(bool value)
			: this(value, false) {
		}

		private readonly bool is_null;
		private readonly bool value;

		/// <summary>
		/// The boolean <c>true</c> value.
		/// </summary>
		public static readonly DeveelDbBoolean True = new DeveelDbBoolean(true); 

		/// <summary>
		/// The boolean <c>false</c> value.
		/// </summary>
		public static readonly DeveelDbBoolean False = new DeveelDbBoolean(false);

		/// <summary>
		/// The boolean <c>null</c> object.
		/// </summary>
		public static readonly DeveelDbBoolean Null = new DeveelDbBoolean(false, true);

		/// <summary>
		/// Gets a value indicating if the boolean value
		/// is intended as <c>null</c>.
		/// </summary>
		public bool IsNull {
			get { return is_null; }
		}

		/// <summary>
		/// Gets the underlying native boolean value.
		/// </summary>
		public bool Value {
			get { return value; }
		}

		public override bool Equals(object obj) {
			if (obj is DBNull || obj == null)
				return is_null;
			bool b;
			if (obj is bool)
				b = (bool)obj;
			else if (obj is DeveelDbBoolean) {
				DeveelDbBoolean dbool = (DeveelDbBoolean) obj;
				if (is_null && dbool.is_null)
					return true;
				b = ((DeveelDbBoolean) obj).value;
			} else
				throw new ArgumentException("Cannot test equality with the type " + obj.GetType());

			return value == b;
		}

		public override int GetHashCode() {
			return is_null ? 0 : value.GetHashCode();
		}

		public override string ToString() {
			return is_null ? "NULL" : value.ToString();
		}

		#region Implementation of IComparable

		public int CompareTo(object obj) {
			if (obj is bool)
				return is_null ? 1 : value.CompareTo((bool) obj);

			if (obj is DBNull || obj == null)
				return is_null ? 0 : -1;

			if (obj is DeveelDbBoolean) {
				DeveelDbBoolean b = (DeveelDbBoolean) obj;
				return is_null && b.is_null ? 0 : value.CompareTo(b.value);
			}

			throw new ArgumentException("Cannot compare the DeveelDbBoolean to " + obj.GetType());
		}

		#endregion

		public static DeveelDbBoolean Parse(string s) {
			if (String.Compare(s, "null", true) == 0)
				return Null;
			return new DeveelDbBoolean(Boolean.Parse(s));
		}

		#region Operators

		public static DeveelDbBoolean operator ==(DeveelDbBoolean a, DeveelDbBoolean b) {
			return a.IsNull || b.IsNull ? Null : new DeveelDbBoolean(a.Equals(b));
		}

		public static DeveelDbBoolean operator !=(DeveelDbBoolean a, DeveelDbBoolean b) {
			return !(a == b);
		}

		public static DeveelDbBoolean operator !(DeveelDbBoolean b) {
			return b.IsNull ? Null : new DeveelDbBoolean(!b.value);
		}

		public static DeveelDbBoolean operator |(DeveelDbBoolean a, DeveelDbBoolean b) {
			return a.IsNull || b.IsNull ? Null : new DeveelDbBoolean(a.value | b.value);
		}

		public static DeveelDbBoolean operator ^(DeveelDbBoolean a, DeveelDbBoolean b) {
			return a.IsNull || b.IsNull ? Null : new DeveelDbBoolean(a.value ^ b.value);
		}

		public static DeveelDbBoolean operator &(DeveelDbBoolean a, DeveelDbBoolean b) {
			return a.IsNull || b.IsNull ? Null : new DeveelDbBoolean(a.value & b.value);
		}

		// special for boolean types...

		public static bool operator true(DeveelDbBoolean b) {
			return b.Value;
		}

		public static bool operator false(DeveelDbBoolean b) {
			return !b.Value;
		}

		// explicit operators...

		public static explicit operator bool (DeveelDbBoolean b) {
			if (b.IsNull)
				throw new NullReferenceException();
			return b.Value;
		}

		public static explicit operator string (DeveelDbBoolean b) {
			return b.ToString();
		}

		// implicit operators...

		public static implicit operator DeveelDbBoolean(bool b) {
			return new DeveelDbBoolean(b);
		}

		public static implicit operator DeveelDbBoolean(string s) {
			return Parse(s);
		}

		#endregion
	}
}