//  
//  TObject.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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
using System.Globalization;
using System.Runtime.Serialization;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="TObject"/> is a strongly typed object in a database engine.
	/// </summary>
	/// <remarks>
	/// A <see cref="TObject"/> must maintain type information (eg. STRING, NUMBER, 
	/// etc) along with the object value being represented itself.
	/// </remarks>
	[Serializable]
	public sealed class TObject : IDeserializationCallback, IComparable, IConvertible {
		/// <summary>
		/// The type of this object.
		/// </summary>
		private readonly TType type;

		/// <summary>
		/// The representation of the object.
		/// </summary>
		private object ob;

		/// <summary>
		/// Constructs a <see cref="TObject"/> for the given <see cref="Data.TType"/>
		/// and a wrapped value.
		/// </summary>
		/// <param name="type">The <see cref="Data.TType"/> of the object.</param>
		/// <param name="ob">The wrapped value of the object.</param>
		public TObject(TType type, Object ob) {
			this.type = type;
			if (ob is String) {
				this.ob = StringObject.FromString((String)ob);
			} else {
				this.ob = ob;
			}
		}

		/// <summary>
		/// Returns the type of this object.
		/// </summary>
		public TType TType {
			get { return type; }
		}

		/// <summary>
		/// Returns true if the object is null.
		/// </summary>
		/// <remarks>
		/// Note that we must still be able to determine type information 
		/// for an object that is NULL.
		/// </remarks>
		public bool IsNull {
			get { return (Object == null); }
		}

		/// <summary>
		/// Returns a <see cref="System.Object"/> that is the data behind this object.
		/// </summary>
		public object Object {
			get { return ob; }
		}

		#region Implicit Operators

		/// <summary>
		/// The equality operator between two <see cref="TObject"/>.
		/// </summary>
		/// <param name="a">The first operand.</param>
		/// <param name="b">The second operand.</param>
		/// <returns>
		/// Returns <b>true</b> if the two operands have the same
		/// <see cref="TType"/> and the same <see cref="Object">value</see>.
		/// </returns>
		/// <seealso cref="Equals"/>
		public static bool operator ==(TObject a, TObject b) {
			if ((object)a == (object)b)
				return true;
			if ((object)a == null && (object)b == null)
				return true;
			if ((object)a == null)
				return false;
			if ((object)b == null)
				return false;

			return a.Equals(b);
		}

		/// <summary>
		/// The inequality operator between two <see cref="TObject"/>.
		/// </summary>
		/// <param name="a">The first operand.</param>
		/// <param name="b">The second operand.</param>
		/// <returns></returns>
		/// <seealso cref="op_Equality"/>
		public static bool operator !=(TObject a, TObject b) {
			return !(a == b);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="a"></param>
		/// <param name="b"></param>
		/// <returns></returns>
		/// <seealso cref="Greater"/>
		public static bool operator >(TObject a, TObject b) {
			if (a == null && b == null)
				return true;
			if (a == null)
				return false;

			return a.Greater(b) == BooleanTrue;
		}

		public static bool operator >=(TObject a, TObject b) {
			if (a == null && b == null)
				return true;
			if (a == null)
				return false;

			return a.GreaterEquals(b) == BooleanTrue;
		}

		public static bool operator <(TObject a, TObject b) {
			return !(a > b);
		}

		public static bool operator <=(TObject a, TObject b) {
			if (a == null && b == null)
				return true;
			if (a == null)
				return false;

			return a.LessEquals(b) == BooleanTrue;
		}

		public static TObject operator |(TObject a, TObject b) {
			if (a == null && b == null)
				return Null;
			if (a == null)
				return b;

			if (a.TType is TStringType)
				return a.Concat(b);
			return a.Or(b);
		}

		public static TObject operator +(TObject a, TObject b) {
			if (a == null && b == null)
				return Null;
			if (a == null)
				throw new ArgumentNullException("a");

			return a.Add(b);
		}

		public static TObject operator -(TObject a, TObject b) {
			if (a == null && b == null)
				return Null;
			if (a == null)
				return Null;

			return a.Subtract(b);
		}

		public static TObject operator /(TObject a, TObject b) {
			if (a == null && b == null)
				return Null;
			if (a == null)
				return Null;

			return a.Divide(b);
		}

		public static TObject operator %(TObject a, TObject b) {
			if (a == null && b == null)
				return Null;
			if (a == null)
				return Null;

			return a.Modulus(b);
		}

		public static TObject operator *(TObject a, TObject b) {
			if (a == null && b == null)
				return Null;
			if (a == null)
				return Null;

			return a.Multiply(a);
		}

		public static TObject operator !(TObject a) {
			if (a == null)
				return Null;

			return a.Not();
		}

		#endregion

		#region Explicit Operators

		public static implicit operator TObject(string s) {
			return GetString(s);
		}

		public static implicit operator TObject(int i) {
			return GetInt4(i);
		}

		public static implicit operator TObject(long l) {
			return GetInt8(l);
		}

		public static implicit operator TObject(bool b) {
			return GetBoolean(b);
		}

		public static implicit operator TObject(DateTime d) {
			return GetDateTime(d);
		}

		public static implicit operator TObject(TimeSpan t) {
			return GetInterval(t);
		}

		public static implicit operator TObject(double d) {
			return GetDouble(d);
		}

		public static implicit operator TObject(BigNumber n) {
			return GetBigNumber(n);
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

		#endregion

		/// <summary>
		/// Returns the approximate memory use of this object in bytes.
		/// </summary>
		/// <remarks>
		/// This is used when the engine is caching objects and we need a 
		/// general indication of how much space it takes up in memory.
		/// </remarks>
		public int ApproximateMemoryUse {
			get { return TType.CalculateApproximateMemoryUse(Object); }
		}

		/// <summary>
		/// Returns true if the type of this object is logically comparable to the
		/// type of the given object.
		/// </summary>
		/// <param name="obj"></param>
		/// <remarks>
		/// For example, VARCHAR and LONGVARCHAR are comparable types.  DOUBLE and 
		/// FLOAT are comparable types.  DOUBLE and VARCHAR are not comparable types.
		/// </remarks>
		public bool ComparableTypes(TObject obj) {
			return TType.IsComparableType(obj.TType);
		}

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
			if (TType is TIntervalType)
				return (TimeSpan) Object;
			if (TType is TNumericType)
				return new TimeSpan(ToBigNumber().ToInt64());
			return TimeSpan.Zero;
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


		public static readonly TObject BooleanTrue = new TObject(TType.BooleanType, true);
		public static readonly TObject BooleanFalse = new TObject(TType.BooleanType, false);
		public static readonly TObject BooleanNull = new TObject(TType.BooleanType, null);

		public static readonly TObject NullObject = new TObject(TType.NullType, null);

		/// <summary>
		/// Returns a TObject of boolean type that is either true or false.
		/// </summary>
		/// <param name="b"></param>
		/// <returns></returns>
		public static TObject GetBoolean(bool b) {
			return b ? BooleanTrue : BooleanFalse;
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given int value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject GetInt4(int val) {
			return GetBigNumber((BigNumber)val);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given long value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject GetInt8(long val) {
			return GetBigNumber((BigNumber)val);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given double value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject GetDouble(double val) {
			return GetBigNumber((BigNumber)val);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given BigNumber value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject GetBigNumber(BigNumber val) {
			return new TObject(TType.NumericType, val);
		}

		/// <summary>
		/// Returns a TObject of VARCHAR type that represents the given 
		/// <see cref="StringObject"/> value.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static TObject GetString(StringObject str) {
			return new TObject(TType.StringType, str);
		}

		/// <summary>
		/// Returns a TObject of VARCHAR type that represents the given 
		/// <see cref="string"/> value.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static TObject GetString(String str) {
			return new TObject(TType.StringType, StringObject.FromString(str));
		}

		/// <summary>
		/// Returns a TObject of DATE type that represents the given time value.
		/// </summary>
		/// <param name="d"></param>
		/// <returns></returns>
		public static TObject GetDateTime(DateTime d) {
			return new TObject(TType.DateType, d);
		}

		/// <summary>
		/// Returns a <see cref="TObject"/> of INTERVAL type that represents the
		/// given interval of time.
		/// </summary>
		/// <param name="t"></param>
		/// <returns></returns>
		public static TObject GetInterval(TimeSpan t) {
			return new TObject(TType.IntervalType, t);
		}

		/// <summary>
		/// Returns a TObject of NULL type that represents a null value.
		/// </summary>
		public static TObject Null {
			get { return NullObject; }
		}

		/// <summary>
		/// Returns a TObject from the given value.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		public static TObject GetObject(Object ob) {
			if (ob == null)
				return Null;
			if (ob is BigNumber)
				return GetBigNumber((BigNumber)ob);
			if (ob is byte)
				return GetBigNumber((byte) ob);
			if (ob is int)
				return GetBigNumber((int)ob);
			if (ob is long)
				return GetBigNumber((long) ob);
			if (ob is float)
				return GetBigNumber((float) ob);
			if (ob is double)
				return GetBigNumber((double) ob);
			if (ob is StringObject)
				return GetString((StringObject)ob);
			if (ob is string)
				return GetString(StringObject.FromString((string) ob));
			if (ob is bool)
				return GetBoolean((Boolean)ob);
			if (ob is DateTime)
				return GetDateTime((DateTime)ob);
			if (ob is ByteLongObject)
				return new TObject(TType.BinaryType, (ByteLongObject)ob);
			if (ob is byte[])
				return new TObject(TType.BinaryType, new ByteLongObject((byte[])ob));
			if (ob is IBlobRef)
				return new TObject(TType.BinaryType, (IBlobRef)ob);
			if (ob is IClobRef)
				return new TObject(TType.StringType, (IClobRef)ob);
			
			throw new ArgumentException("Don't know how to convert object type " + ob.GetType());
		}

		/// <summary>
		/// Compares this object with the given object (which is of a logically
		/// comparable type).
		/// </summary>
		/// <remarks>
		/// This cannot be used to compare null values so it assumes that checks
		/// for null have already been made.
		/// </remarks>
		/// <returns>
		/// Returns 0 if the value of the objects are equal, -1 if this object is smaller 
		/// than the given object, and 1 if this object is greater than the given object.
		/// </returns>
		public int CompareToNoNulls(TObject tob) {
			TType ttype = TType;
			// Strings must be handled as a special case.
			if (ttype is TStringType) {
				// We must determine the locale to compare against and use that.
				TStringType stype = (TStringType)type;
				// If there is no locale defined for this type we use the locale in the
				// given type.
				if (stype.Locale == null) {
					ttype = tob.TType;
				}
			}
			return ttype.Compare(Object, tob.Object);
		}


		/// <summary>
		/// Compares this object with the given object (which is of a logically
		/// comparable type).
		/// </summary>
		/// <remarks>
		/// This compares <c>NULL</c> values before non null values, and null values are
		/// equal.
		/// </remarks>
		/// <returns>
		/// Returns 0 if the value of the objects are equal, -1 if this object is smaller 
		/// than the given object, 1 if this object is greater than the given object.
		/// </returns>
		/// <seealso cref="CompareToNoNulls"/>
		public int CompareTo(TObject tob) {
			// If this is null
			if (IsNull) {
				// and value is null return 0 return less
				if (tob.IsNull)
					return 0;
				return -1;
			}
			// If this is not null and value is null return +1
			if (tob.IsNull)
				return 1;
			// otherwise both are non null so compare normally.
			return CompareToNoNulls(tob);
		}

		int IComparable.CompareTo(object obj) {
			if (!(obj is TObject))
				throw new ArgumentException();
			return CompareTo((TObject)obj);
		}

		/// <inheritdoc/>
		/// <exception cref="ApplicationException">
		/// It's not clear what we would be testing the equality of with this method.
		/// </exception>
		public override bool Equals(object obj) {
			// throw new ApplicationException("equals method should not be used.");
			TObject tobj = obj as TObject;
			if (tobj == null)
				return false;
			if (!type.Equals(tobj.type))
				return false;
			if (ob == null && tobj.ob == null)
				return true;
			if (ob == null && tobj.ob != null)
				return false;
			if (ob == null)
				return false;
			return ob.Equals(tobj.ob);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return type.GetHashCode() ^ (ob == null ? 0 : ob.GetHashCode());
		}

		///<summary>
		/// Verifies if the given object is equivalent to the current
		/// instance of the <see cref="TObject"/>.
		///</summary>
		///<param name="obj">The <see cref="TObject"/> to verify.</param>
		/// <remarks>
		/// This method is different from <see cref="Equals"/> in which it
		/// compares the type and values of the current <see cref="TObject"/> 
		/// and the given <paramref name="obj">one</paramref>.
		/// </remarks>
		///<returns>
		/// Returns <b>true</b> if this object is equivalent to the given 
		/// <paramref name="obj">object</paramref>, or <b>false</b> otherwise.
		/// </returns>
		public bool ValuesEqual(TObject obj) {
			if (this == obj) {
				return true;
			}
			if (TType.IsComparableType(obj.TType)) {
				return CompareTo(obj) == 0;
			}
			return false;
		}



		// ---------- Object operators ----------

		#region Operators

		/*
		public static TObject operator |(TObject a, TObject b) {
			return a.Or(b);
		}

		public static TObject operator +(TObject a, TObject b) {
			return a.Add(b);
		}

		public static TObject operator -(TObject a, TObject b) {
			return a.Subtract(b);
		}

		public static TObject operator *(TObject a, TObject b) {
			return a.Multiply(b);
		}

		public static TObject operator /(TObject a, TObject b) {
			return a.Divide(b);
		}

		public static TObject operator >(TObject a, TObject b) {
			return a.Greater(b);
		}

		public static TObject operator >=(TObject a, TObject b) {
			return a.GreaterEquals(b);
		}

		public static TObject operator <(TObject a, TObject b) {
			return a.Less(b);
		}

		public static TObject operator <=(TObject a, TObject b) {
			return a.LessEquals(b);
		}

		public static TObject operator !(TObject a) {
			return a.Not();
		}
		*/

		#endregion

		/// <summary>
		/// Bitwise <b>OR</b> operation of this object with the given object.
		/// </summary>
		/// <param name="val">The operand object of the operation.</param>
		/// <returns>
		/// If <see cref="Type"/> is a <see cref="TNumericType"/>, it returns the result 
		/// of the bitwise-or between this object value and the given one. If either numeric 
		/// value has a scale of 1 or greater then it returns <b>null</b>. If this or the given 
		/// object is not a numeric type then it returns <b>null</b>. If either this object or 
		/// the given object is <c>NULL</c>, then the <c>NULL</c> object is returned.
		/// </returns>
		public TObject Or(TObject val) {
			BigNumber v1 = ToBigNumber();
			BigNumber v2 = val.ToBigNumber();
			TType result_type = TType.GetWidestType(TType, val.TType);

			if (v1 == null || v2 == null) {
				return new TObject(result_type, null);
			}

			return new TObject(result_type, v1.BitWiseOr(v2));
		}

		/// <summary>
		/// Performs an addition between the value of this object and that of the specified.
		/// </summary>
		/// <returns>
		/// If this or the given object is not a numeric or interval type then it 
		/// returns null. If either this object or the given object is <c>NULL</c>, 
		/// then the <c>NULL</c> object is returned. Returns the mathematical addition
		/// between this numeric value and the given one if <see cref="TObject.TType"/>
		/// is <see cref="TNumericType"/>.
		/// </returns>
		public TObject Add(TObject val) {
			if (TType is TDateType) {
				DateTime v1 = ToDateTime();
				if (val.TType is TNumericType) {
					BigNumber v2 = val.ToBigNumber();
					return new TObject(TType.DateType, v1.AddMilliseconds(v2.ToDouble()));
				} else if (val.TType is TIntervalType) {
					TimeSpan v2 = val.ToTimeSpan();
					return new TObject(TType.DateType, v1.Add(v2));
				}
			} else if (TType is TIntervalType) {
				TimeSpan v1 = ToTimeSpan();
				if (val.TType is TIntervalType) {
					TimeSpan v2 = val.ToTimeSpan();
					return new TObject(TType, v1.Add(v2));
				} else if (val.TType is TNumericType) {
					BigNumber v2 = val.ToBigNumber();
					return new TObject(TType, v1.Add(new TimeSpan(v2.ToInt32() * TimeSpan.TicksPerMillisecond)));
				}
			} else if (TType is TNumericType) {
				BigNumber v1 = ToBigNumber();
				BigNumber v2 = val.ToBigNumber();
				TType result_type = TType.GetWidestType(TType, val.TType);

				if (v1 == null || v2 == null) {
					return new TObject(result_type, null);
				}

				return new TObject(result_type, v1.Add(v2));
			} else if (TType is TStringType) {
				if (!(val.TType is TStringType))
					val = val.CastTo(TType.StringType);

				return Concat(val);
			}

			throw new InvalidOperationException();
		}

		/// <summary>
		/// Performs a subtraction of the value of the current object to the given one.
		/// </summary>
		/// <returns>
		/// If this or the given object is not a numeric or interval type then it returns null.
		/// If either this object or the given object is <c>NULL</c>, then the <c>NULL</c> 
		/// object is returned.
		/// </returns>
		public TObject Subtract(TObject val) {
			if (TType is TDateType) {
				DateTime v1 = ToDateTime();
				if (val.TType is TNumericType) {
					BigNumber v2 = val.ToBigNumber();
					return new TObject(TType.DateType, v1.Subtract(new TimeSpan(v2.ToInt32() * TimeSpan.TicksPerMillisecond)));
				} else if (val.TType is TIntervalType) {
					TimeSpan v2 = val.ToTimeSpan();
					return new TObject(TType.DateType, v1.Subtract(v2));
				} else if (val.TType is TDateType) {
					DateTime v2 = val.ToDateTime();
					return new TObject(TType.IntervalType, v1.Subtract(v2));
				}
			} else if (TType is TIntervalType) {
				TimeSpan v1 = ToTimeSpan();
				if (val.TType is TIntervalType) {
					TimeSpan v2 = val.ToTimeSpan();
					return new TObject(TType, v1.Add(v2));
				} else if (val.TType is TNumericType) {
					BigNumber v2 = val.ToBigNumber();
					return new TObject(TType, v1.Add(new TimeSpan(v2.ToInt32() * TimeSpan.TicksPerMillisecond)));
				}
			} else if (TType is TNumericType) {
				BigNumber v1 = ToBigNumber();
				BigNumber v2 = val.ToBigNumber();
				TType result_type = TType.GetWidestType(TType, val.TType);

				if (v1 == null || v2 == null) {
					return new TObject(result_type, null);
				}

				return new TObject(result_type, v1.Subtract(v2));
			}

			throw new InvalidOperationException();
		}

		/// <summary>
		/// Mathematical multiply of this object to the given object.
		/// </summary>
		/// <returns>
		/// If this or the given object is not a numeric type then it returns null.
		/// If either this object or the given object is NULL, then the NULL object
		/// is returned.
		/// </returns>
		public TObject Multiply(TObject val) {
			BigNumber v1 = ToBigNumber();
			BigNumber v2 = val.ToBigNumber();
			TType result_type = TType.GetWidestType(TType, val.TType);

			if (v1 == null || v2 == null) {
				return new TObject(result_type, null);
			}

			return new TObject(result_type, v1.Multiply(v2));
		}

		/// <summary>
		/// Mathematical division of this object to the given object.
		/// </summary>
		/// <returns>
		/// If this or the given object is not a numeric type then it returns null.
		/// If either this object or the given object is NULL, then the NULL object
		/// is returned.
		/// </returns>
		public TObject Divide(TObject val) {
			BigNumber v1 = ToBigNumber();
			BigNumber v2 = val.ToBigNumber();
			TType result_type = TType.GetWidestType(TType, val.TType);

			if (v1 == null || v2 == null) {
				return new TObject(result_type, null);
			}

			return new TObject(result_type, v1.Divide(v2));
		}

		public TObject Modulus(TObject val) {
			BigNumber v1 = ToBigNumber();
			BigNumber v2 = val.ToBigNumber();
			TType result_type = TType.GetWidestType(TType, val.TType);

			if (v1 == null || v2 == null) {
				return new TObject(result_type, null);
			}

			return new TObject(result_type, v1.Modulus(v2));
		}

		/// <summary>
		/// String concat of this object to the given object.
		/// </summary>
		/// <returns>
		/// If this or the given object is not a string type then it returns null.  
		/// If either this object or the given object is NULL, then the NULL object 
		/// is returned.
		/// </returns>
		/// <remarks>
		/// This operator always returns an object that is a VARCHAR string type of
		/// unlimited size with locale inherited from either this or val depending
		/// on whether the locale information is defined or not.
		/// </remarks>
		public TObject Concat(TObject val) {
			// If this or val is null then return the null value
			if (IsNull)
				return this;
			if (val.IsNull)
				return val;

			TType tt1 = TType;
			TType tt2 = val.TType;

			if (tt1 is TStringType &&
				tt2 is TStringType) {
				// Pick the first locale,
				TStringType st1 = (TStringType)tt1;
				TStringType st2 = (TStringType)tt2;

				CultureInfo str_locale = null;
				Text.CollationStrength str_strength = 0;
				Text.CollationDecomposition str_decomposition = 0;

				if (st1.Locale != null) {
					str_locale = st1.Locale;
					str_strength = st1.Strength;
					str_decomposition = st1.Decomposition;
				} else if (st2.Locale != null) {
					str_locale = st2.Locale;
					str_strength = st2.Strength;
					str_decomposition = st2.Decomposition;
				}

				TStringType dest_type = st1;
				if (str_locale != null) {
					dest_type = new TStringType(SqlType.VarChar, -1,
											 str_locale, str_strength, str_decomposition);
				}

				return new TObject(dest_type,
						StringObject.FromString(ToStringValue() + val.ToStringValue()));

			}

			// Return null if LHS or RHS are not strings
			return new TObject(tt1, null);
		}

		/// <summary>
		/// Comparison of this object and the given object.
		/// </summary>
		/// <remarks>
		/// The compared objects must be the same type otherwise it returns false. 
		/// This is able to compare null values.
		/// </remarks>
		public TObject Is(TObject val) {
			if (IsNull && val.IsNull)
				return BooleanTrue;
			if (ComparableTypes(val))
				return GetBoolean(CompareTo(val) == 0);
			// Not comparable types so return false
			return BooleanFalse;
		}

		///<summary>
		/// Comparison of this object and the given object.
		///</summary>
		///<param name="val"></param>
		/// <remarks>
		/// The compared objects must be the same type otherwise it returns 
		/// null (doesn't know). If either this object or the given object is 
		/// <c>NULL</c> then <c>NULL</c> is returned.
		/// </remarks>
		///<returns></returns>
		public TObject IsEqual(TObject val) {
			// Check the types are comparable
			if (ComparableTypes(val) && !IsNull && !val.IsNull) {
				return GetBoolean(CompareToNoNulls(val) == 0);
			}
			// Not comparable types so return null
			return BooleanNull;
		}

		///<summary>
		/// Comparison of this object and the given object.
		///</summary>
		///<param name="val"></param>
		/// <remarks>
		/// The compared objects must be the same type otherwise it returns 
		/// null (doesn't know). If either this object or the given object is 
		/// <c>NULL</c> then <c>NULL</c> is returned.
		/// </remarks>
		///<returns></returns>
		public TObject IsNotEqual(TObject val) {
			// Check the types are comparable
			if (ComparableTypes(val) && !IsNull && !val.IsNull) {
				return GetBoolean(CompareToNoNulls(val) != 0);
			}
			// Not comparable types so return null
			return BooleanNull;
		}

		/// <summary>
		/// Comparison of this object and the given object.
		/// </summary>
		/// <remarks>
		/// The compared objects must be the same type otherwise it returns 
		/// null (doesn't know).
		/// </remarks>
		/// <returns>
		/// If either this object or the given object is NULL then NULL is 
		/// returned.
		/// </returns>
		public TObject Greater(TObject val) {
			// Check the types are comparable
			if (ComparableTypes(val) && !IsNull && !val.IsNull) {
				return GetBoolean(CompareToNoNulls(val) > 0);
			}
			// Not comparable types so return null
			return BooleanNull;
		}

		/// <summary>
		/// Comparison of this object and the given object.
		/// </summary>
		/// <remarks>
		/// The compared objects must be the same type otherwise it returns 
		/// null (doesn't know).
		/// </remarks>
		/// <returns>
		/// If either this object or the given object is NULL then NULL is 
		/// returned.
		/// </returns>
		public TObject GreaterEquals(TObject val) {
			// Check the types are comparable
			if (ComparableTypes(val) && !IsNull && !val.IsNull) {
				return GetBoolean(CompareToNoNulls(val) >= 0);
			}
			// Not comparable types so return null
			return BooleanNull;
		}

		/// <summary>
		/// Comparison of this object and the given object.
		/// </summary>
		/// <remarks>
		/// The compared objects must be the same type otherwise it returns 
		/// null (doesn't know).</remarks>
		/// <returns>
		/// If either this object or the given object is NULL then NULL is 
		/// returned.
		/// </returns>
		public TObject Less(TObject val) {
			// Check the types are comparable
			if (ComparableTypes(val) && !IsNull && !val.IsNull) {
				return GetBoolean(CompareToNoNulls(val) < 0);
			}
			// Not comparable types so return null
			return BooleanNull;
		}

		/// <summary>
		/// Comparison of this object and the given object.
		/// </summary>
		/// <remarks>
		/// The compared objects must be the same type otherwise it 
		/// returns null (doesn't know).</remarks>
		/// <returns>
		/// If either this object or the given object is NULL then NULL is 
		/// returned.
		/// </returns>
		public TObject LessEquals(TObject val) {
			// Check the types are comparable
			if (ComparableTypes(val) && !IsNull && !val.IsNull) {
				return GetBoolean(CompareToNoNulls(val) <= 0);
			}
			// Not comparable types so return null
			return BooleanNull;
		}

		/// <summary>
		/// Equality test for the similarity of two strings based on
		/// the <c>SOUNDEX</c> algorithm.
		/// </summary>
		/// <param name="val">The value to compare.</param>
		/// <remarks>
		/// This method compares the two values based on the US-en
		/// vocabulary mappings.
		/// </remarks>
		/// <returns>
		/// Returns a boolean value indicating if the current object and the
		/// given string value sounds similarly.
		/// </returns>
		public TObject SoundsLike(TObject val) {
			if (!(val.TType is TStringType))
				val = val.CastTo(TType.StringType);

			//TODO: support more languages...
			Text.Soundex soundex = Text.Soundex.UsEnglish;

			string v1 = ToStringValue();
			string v2 = val.ToStringValue();

			string sd1 = soundex.Compute(v1);
			string sd2 = soundex.Compute(v2);
			return GetBoolean(String.Compare(sd1, sd2, false) == 0);
		}


		/// <summary>
		/// Performs a logical NOT on this value.
		/// </summary>
		/// <returns>
		/// </returns>
		public TObject Not() {
			// If type is null
			if (IsNull) {
				return this;
			}
			bool isNull;
			bool b = ToBoolean(out isNull);
			if (!isNull)
				return GetBoolean(!b);
			return BooleanNull;
		}


		// ---------- Casting methods -----------

		///<summary>
		/// Returns a TObject of the given type and with the given object.
		///</summary>
		///<param name="type"></param>
		///<param name="ob"></param>
		/// <remarks>
		///  If the object is not of the right type then it is cast to the correct type.
		/// </remarks>
		///<returns></returns>
		public static TObject CreateAndCastFromObject(TType type, Object ob) {
			return new TObject(type, TType.CastObjectToTType(ob, type));
		}

		/// <summary>
		/// Casts this object to the given type and returns a new <see cref="TObject"/>.
		/// </summary>
		/// <param name="cast_to_type"></param>
		/// <returns></returns>
		public TObject CastTo(TType cast_to_type) {
			Object obj = Object;
			return CreateAndCastFromObject(cast_to_type, obj);
		}


		/// <inheritdoc/>
		public override string ToString() {
			return IsNull ? "NULL" : Object.ToString();
		}

		void IDeserializationCallback.OnDeserialization(object sender) {
			if (ob is string)
				ob = StringObject.FromString((string) ob);
		}

		#region Implementation of IConvertible

		TypeCode IConvertible.GetTypeCode() {
			return TypeCode.Object;
		}

		bool IConvertible.ToBoolean(IFormatProvider provider) {
			return ToBoolean();
		}

		char IConvertible.ToChar(IFormatProvider provider) {
			string s = ToStringValue();
			return (s == null || s.Length == 0 ? '\0' : s[0]);
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
			return (float) ToBigNumber().ToDouble();
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
			if (conversionType == typeof(BigNumber))
				return ToBigNumber();

			throw new NotSupportedException("Cannot convert to type '" + conversionType + "'.");
		}

		#endregion
	}
}