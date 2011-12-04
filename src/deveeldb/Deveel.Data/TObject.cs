// 
//  Copyright 2010-2011  Deveel
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
	/// <summary>
	/// A <see cref="TObject"/> is a strongly typed object in a database engine.
	/// </summary>
	/// <remarks>
	/// A <see cref="TObject"/> must maintain type information (eg. STRING, NUMBER, 
	/// etc) along with the object value being represented itself.
	/// </remarks>
	[Serializable]
	public sealed partial class TObject : IComparable {
		/// <summary>
		/// The type of this object.
		/// </summary>
		private readonly TType type;

		/// <summary>
		/// The representation of the object.
		/// </summary>
		private readonly object ob;

		/// <summary>
		/// Constructs a <see cref="TObject"/> for the given <see cref="Data.TType"/>
		/// and a wrapped value.
		/// </summary>
		/// <param name="type">The <see cref="Data.TType"/> of the object.</param>
		/// <param name="ob">The wrapped value of the object.</param>
		public TObject(TType type, object ob) {
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

		public bool IsReference {
			get { return ob is IRef; }
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
		public bool IsComparableTo(TObject obj) {
			return TType.IsComparableType(obj.TType);
		}

		public static readonly TObject BooleanTrue = new TObject(TType.BooleanType, true);
		public static readonly TObject BooleanFalse = new TObject(TType.BooleanType, false);
		public static readonly TObject BooleanNull = new TObject(TType.BooleanType, null);

		/// <summary>
		/// A TObject of NULL type that represents a null value.
		/// </summary>
		public static readonly TObject Null = new TObject(TType.NullType, null);

		/// <summary>
		/// Returns a TObject of boolean type that is either true or false.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateBoolean(bool value) {
			return value ? BooleanTrue : BooleanFalse;
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given int value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateInt4(int value) {
			return CreateBigNumber(value);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given long value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateInt8(long value) {
			return CreateBigNumber(value);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given double value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateDouble(double value) {
			return CreateBigNumber(value);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given BigNumber value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateBigNumber(BigNumber value) {
			return new TObject(TType.NumericType, value);
		}

		/// <summary>
		/// Returns a TObject of VARCHAR type that represents the given 
		/// <see cref="StringObject"/> value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateString(StringObject value) {
			return new TObject(TType.StringType, value);
		}

		/// <summary>
		/// Returns a TObject of VARCHAR type that represents the given 
		/// <see cref="string"/> value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateString(string value) {
			return new TObject(TType.StringType, StringObject.FromString(value));
		}

		/// <summary>
		/// Returns a TObject of DATE type that represents the given time value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateDateTime(DateTime value) {
			return new TObject(TType.DateType, value);
		}

		/// <summary>
		/// Returns a <see cref="TObject"/> of INTERVAL DAY TO SECOND type that 
		/// represents the given interval of time.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateInterval(TimeSpan value) {
			return new TObject(TType.GetIntervalType(SqlType.DayToSecond), new Interval(value.Days, value.Hours, value.Minutes, value.Seconds));
		}

		public static TObject CreateInterval(Interval value) {
			return new TObject(TType.IntervalType, value);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateQueryPlan(IQueryPlanNode value) {
			return new TObject(TType.QueryPlanType, value);
		}


		/// <summary>
		/// Returns a TObject from the given value.
		/// </summary>
		/// <param name="value"></param>
		/// <returns></returns>
		public static TObject CreateObject(object value) {
			if (value == null)
				return Null;
			if (value is BigNumber)
				return CreateBigNumber((BigNumber)value);
			if (value is byte)
				return CreateBigNumber((byte) value);
			if (value is int)
				return CreateBigNumber((int)value);
			if (value is long)
				return CreateBigNumber((long) value);
			if (value is float)
				return CreateBigNumber((float) value);
			if (value is double)
				return CreateBigNumber((double) value);
			if (value is StringObject)
				return CreateString((StringObject)value);
			if (value is string)
				return CreateString(StringObject.FromString((string) value));
			if (value is bool)
				return CreateBoolean((Boolean)value);
			if (value is DateTime)
				return CreateDateTime((DateTime)value);
			if (value is ByteLongObject)
				return new TObject(TType.BinaryType, (ByteLongObject)value);
			if (value is byte[])
				return new TObject(TType.BinaryType, new ByteLongObject((byte[])value));
			if (value is IBlobRef)
				return new TObject(TType.BinaryType, value);
			if (value is IClobRef)
				return new TObject(TType.StringType, value);
			
			throw new ArgumentException("Don't know how to convert object type " + value.GetType());
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


		/// <inheritdoc/>
		public override string ToString() {
			return IsNull ? "NULL" : Object.ToString();
		}
	}
}