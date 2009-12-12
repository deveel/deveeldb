//  
//  Number.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Paul Fisher
//       John Keiser
//       Warren Levy
//       Eric Blake <ebb9@email.byu.edu>
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

// This class was derived from the GNU Classpath's java.lang.Number

using System;

namespace Deveel.Math {
	/// <summary>
	/// 
	/// </summary>
	[Serializable]
	public abstract class Number : IConvertible, IComparable {
		#region ctor
		protected Number() {
		}
		#endregion

		#region Fields
		internal static char[] Digits = new char[]{
		                                          	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
		                                          	'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
		                                          	'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
		                                          	'u', 'v', 'w', 'x', 'y', 'z',
		                                          };
		#endregion

		#region IConvertible Implementations
		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return (bool)ToType(typeof(bool), provider);
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			return (char)ToType(typeof(char), provider);
		}

		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			return (sbyte)ToType(typeof(sbyte), provider);
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return (byte)ToType(typeof(byte), provider);
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return (short)ToType(typeof(short), provider);
		}

		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			return (ushort)ToType(typeof(ushort), provider);
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return (int)ToType(typeof(int), provider);
		}

		uint IConvertible.ToUInt32(IFormatProvider provider) {
			return (uint)ToType(typeof(uint), provider);
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return (long)ToType(typeof(long), provider);
		}

		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			return (ulong)ToType(typeof(ulong), provider);
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return (float)ToType(typeof(float), provider);
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return (double)ToType(typeof(double), provider);
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			return (decimal)ToType(typeof(decimal), provider);
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			return (DateTime)ToType(typeof(DateTime), provider);
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return (string)ToType(typeof(string), provider);
		}
		#endregion

		#region Public Methods
		public abstract int ToInt32();
		public abstract long ToInt64();
		public abstract float ToSingle();
		public abstract double ToDouble();
		public abstract override string ToString();

		public abstract int CompareTo(object obj);

		public virtual byte ToByte() {
			return (byte)ToInt32();
		}

		public virtual short ToInt16() {
			return (short)ToInt32();
		}

		public virtual TypeCode GetTypeCode() {
			return TypeCode.Object;
		}

		public virtual object ToType(Type conversionType, IFormatProvider provider) {
			if (conversionType == typeof(int))
				return ToInt32();
			if (conversionType == typeof(long))
				return ToInt64();
			if (conversionType == typeof(short))
				return ToInt16();
			if (conversionType == typeof(float))
				return ToSingle();
			if (conversionType == typeof(double))
				return ToDouble();
			if (conversionType == typeof(string))
				return ToString();
			if (conversionType == typeof(byte))
				return ToByte();
			throw new NotSupportedException("Unable to convert from '"+GetType()+"' to '"+conversionType+"'.");
		}
		#endregion
	}
}