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

namespace Deveel.Data {
	public sealed partial class TObject : IConvertible {
		/// <summary>
		/// Gets the value of the object as a <see cref="BigNumber"/>.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="BigNumber"/> if <see cref="TObject.TType"/> is a
		/// <see cref="TNumericType"/>, <b>null</b> otherwise.
		/// </returns>
		public BigNumber ToBigNumber() {
			if (TType is TNumericType)
				return (BigNumber)Object;
			return null;
		}

		/// <summary>
		/// Gets the value of the object as a <see cref="TimeSpan"/>.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="TimeSpan"/> if <see cref="TObject.TType"/> is a
		/// <see cref="TIntervalType"/>, <c>NULL</c> otherwise.
		/// </returns>
		public TimeSpan ToTimeSpan() {
			if (TType is TIntervalType) {
				Interval interval = (Interval)Object;
				return interval.ToTimeSpan();
			}
			if (TType is TNumericType)
				return new TimeSpan(ToBigNumber().ToInt64());
			return TimeSpan.Zero;
		}

		public Interval ToInterval() {
			if (TType is TIntervalType)
				return (Interval)Object;
			return Interval.Zero;
		}

		public DateTime ToDateTime() {
			if (TType is TDateType)
				return (DateTime)Object;
			if (TType is TNumericType)
				return new DateTime(ToBigNumber().ToInt64(), DateTimeKind.Utc);
			return DateTime.MinValue;
		}

		/// <summary>
		/// Gets the value of the object as a <see cref="Number"/>.
		/// </summary>
		/// <param name="isNull"></param>
		/// <returns>
		/// Returns a <see cref="Boolean"/> if <see cref="TObject.TType"/> is a
		/// <see cref="TBooleanType"/>.
		/// </returns>
		/// <exception cref="InvalidCastException">
		/// If the value wrapped by this object is not a <see cref="TBooleanType"/>.
		/// </exception>
		public bool ToBoolean(out bool isNull) {
			if (TType is TBooleanType) {
				object value = Object;
				isNull = (value == null);
				return (value == null ? false : (bool)value);
			}
			isNull = true;
			return false;
		}

		/// <summary>
		/// Gets the value of the object as a <see cref="Number"/>.
		/// </summary>
		/// <returns>
		/// Returns a <see cref="Boolean"/> if <see cref="TObject.TType"/> is a
		/// <see cref="TBooleanType"/>.
		/// </returns>
		/// <exception cref="InvalidCastException">
		/// If the value wrapped by this object is not a <see cref="TBooleanType"/>.
		/// </exception>
		/// <seealso cref="ToBoolean(out bool)"/>
		public bool ToBoolean() {
			bool isNull;
			return ToBoolean(out isNull);
		}

		/// <summary>
		/// Returns the String of this object if this object is a string type.
		/// </summary>
		/// <remarks>
		/// If the object is not a string type or is NULL then a null object is
		/// returned.  This method must not be used to cast from a type to a string.
		/// </remarks>
		/// <returns></returns>
		public String ToStringValue() {
			if (TType is TStringType) {
				return Object.ToString();
			}
			return null;
		}

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return ToBoolean();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			string s = ToStringValue();
			return (String.IsNullOrEmpty(s) ? '\0' : s[0]);
		}

		[CLSCompliant(false)]
		sbyte IConvertible.ToSByte(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		byte IConvertible.ToByte(IFormatProvider provider) {
			return ToBigNumber().ToByte();
		}

		short IConvertible.ToInt16(IFormatProvider provider) {
			return ToBigNumber().ToInt16();
		}

		[CLSCompliant(false)]
		ushort IConvertible.ToUInt16(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		int IConvertible.ToInt32(IFormatProvider provider) {
			return ToBigNumber().ToInt32();
		}

		[CLSCompliant(false)]
		uint IConvertible.ToUInt32(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		long IConvertible.ToInt64(IFormatProvider provider) {
			return ToBigNumber().ToInt64();
		}

		[CLSCompliant(false)]
		ulong IConvertible.ToUInt64(IFormatProvider provider) {
			throw new NotSupportedException();
		}

		float IConvertible.ToSingle(IFormatProvider provider) {
			return (float)ToBigNumber().ToDouble();
		}

		double IConvertible.ToDouble(IFormatProvider provider) {
			return ToBigNumber().ToDouble();
		}

		decimal IConvertible.ToDecimal(IFormatProvider provider) {
			//TODO:
			throw new NotImplementedException();
		}

		DateTime IConvertible.ToDateTime(IFormatProvider provider) {
			return ToDateTime();
		}

		string IConvertible.ToString(IFormatProvider provider) {
			return ToStringValue();
		}

		object IConvertible.ToType(Type conversionType, IFormatProvider provider) {
			// standard comversions...
			if (conversionType == typeof(bool))
				return (this as IConvertible).ToBoolean(provider);
			if (conversionType == typeof(byte))
				return (this as IConvertible).ToByte(provider);
			if (conversionType == typeof(short))
				return (this as IConvertible).ToInt16(provider);
			if (conversionType == typeof(int))
				return (this as IConvertible).ToInt32(provider);
			if (conversionType == typeof(long))
				return (this as IConvertible).ToInt64(provider);
			if (conversionType == typeof(float))
				return (this as IConvertible).ToSingle(provider);
			if (conversionType == typeof(double))
				return (this as IConvertible).ToDouble(provider);
			if (conversionType == typeof(decimal))
				return (this as IConvertible).ToDecimal(provider);
			if (conversionType == typeof(char))
				return (this as IConvertible).ToChar(provider);
			if (conversionType == typeof(DateTime))
				return (this as IConvertible).ToDateTime(provider);
			if (conversionType == typeof(string))
				return (this as IConvertible).ToString(provider);

			// non standard...
			if (conversionType == typeof(TimeSpan))
				return ToTimeSpan();
			if (conversionType == typeof(Interval))
				return ToInterval();
			if (conversionType == typeof(BigNumber))
				return ToBigNumber();

			throw new NotSupportedException("Cannot convert to type '" + conversionType + "'.");
		}

		public static implicit operator TObject(string s) {
			return CreateString(s);
		}

		public static implicit operator TObject(int i) {
			return CreateInt4(i);
		}

		public static implicit operator TObject(long l) {
			return CreateInt8(l);
		}

		public static implicit operator TObject(bool b) {
			return CreateBoolean(b);
		}

		public static implicit operator TObject(DateTime d) {
			return CreateDateTime(d);
		}

		public static implicit operator TObject(TimeSpan t) {
			return CreateInterval(t);
		}

		public static implicit operator TObject(Interval i) {
			return CreateInterval(i);
		}

		public static implicit operator TObject(double d) {
			return CreateDouble(d);
		}

		public static implicit operator TObject(BigNumber n) {
			return CreateBigNumber(n);
		}

		// backward
		public static implicit operator String(TObject obj) {
			return obj.ToString();
		}

		public static implicit operator Int64(TObject obj) {
			return obj.ToBigNumber().ToInt64();
		}

		public static implicit operator Int32(TObject obj) {
			return obj.ToBigNumber().ToInt32();
		}

		public static implicit operator Double(TObject obj) {
			return obj.ToBigNumber().ToDouble();
		}

		public static implicit operator BigNumber(TObject obj) {
			return obj.ToBigNumber();
		}

		public static implicit operator TimeSpan(TObject obj) {
			return obj.ToTimeSpan();
		}

		public static implicit operator Interval(TObject obj) {
			return obj.ToInterval();
		}
	}
}