// 
//  DeveelDbString.cs
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
using System.Globalization;

namespace Deveel.Data.Client {
	public struct DeveelDbString : IComparable, INullable, ISizeable {
		public DeveelDbString(string value) {
			this.value = value;
			isNull = (value == null);
		}

		private readonly bool isNull;
		private readonly string value;

		public static readonly DeveelDbString Null = new DeveelDbString(null);

		public static readonly DeveelDbString Empty = new DeveelDbString(String.Empty);

		public int Length {
			get { return isNull ? 0 : value.Length; }
		}

		public char this[int index] {
			get { return isNull ? '\0' : value[index]; }
		}

		public string Value {
			get { return value; }
		}

		#region Implementation of IComparable

		public int CompareTo(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull ? 0 : -1;
			if (obj is string) {
				string s = (string) obj;
				return value == null ? 1 : value.CompareTo(s);
			}
			if (obj is DeveelDbString) {
				DeveelDbString s = (DeveelDbString) obj;
				return IsNull && s.IsNull ? 0 : (value == null ? 1 : value.CompareTo(s.value));
			}

			throw new ArgumentException("Cannot compare a DeveelDB string to " + obj.GetType());
		}

		#endregion

		#region Implementation of INullable

		public bool IsNull {
			get { return isNull; }
		}

		#endregion

		public override bool Equals(object obj) {
			if (obj == null || obj == DBNull.Value)
				return isNull;
			if (obj is string)
				return value == (string) obj;
			if (!(obj is DeveelDbString))
				throw new ArgumentException("Cannot test the equality between a DeveelDB string and " + obj.GetType());

			DeveelDbString s = (DeveelDbString) obj;
			return isNull && s.isNull ? true : value == s.value;
		}

		public override int GetHashCode() {
			return value == null ? 0 : value.GetHashCode();
		}

		public override string ToString() {
			return isNull ? null : value;
		}

		public char[] ToCharArray() {
			return isNull ? null : value.ToCharArray();
		}

		#region Operators

		public static DeveelDbBoolean operator ==(DeveelDbString a, DeveelDbString b) {
			return a.IsNull && b.IsNull ? DeveelDbBoolean.Null : new DeveelDbBoolean(a.Equals(b));
		}

		public static DeveelDbBoolean operator !=(DeveelDbString a, DeveelDbString b) {
			return !(a == b);
		}

		public static DeveelDbString operator +(DeveelDbString a, DeveelDbString b) {
			return a.IsNull ? Null : new DeveelDbString(a.value + b.value);
		}

		public static DeveelDbBoolean operator >(DeveelDbString a, DeveelDbString b) {
			return a.IsNull
			       	? DeveelDbBoolean.Null
			       	: String.Compare(a.value, b.value, CultureInfo.InvariantCulture, CompareOptions.Ordinal) > 0;
		}

		public static DeveelDbBoolean operator <(DeveelDbString a, DeveelDbString b) {
			return a.IsNull
			       	? DeveelDbBoolean.Null
			       	: String.Compare(a.value, b.value, CultureInfo.InvariantCulture, CompareOptions.Ordinal) < 0;
		}

		// explicit operators ...

		public static explicit operator string (DeveelDbString s) {
			return s.Value;
		}

		// implicit operators ...

		public static implicit operator DeveelDbString(string s) {
			return new DeveelDbString(s);
		}

		#endregion

		#region Implementation of ISizeable

		int ISizeable.Size {
			get { return Length; }
		}

		#endregion
	}
}