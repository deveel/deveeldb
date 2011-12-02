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
using System.Runtime.Serialization;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="TObject"/> is a strongly typed object in a database engine.
	/// </summary>
	/// <remarks>
	/// A <see cref="TObject"/> must maintain type information (eg. STRING, NUMBER, 
	/// etc) along with the object value being represented itself.
	/// </remarks>
	[Serializable]
	public sealed partial class TObject : IExpressionElement, IDeserializationCallback, IComparable, IComparable<TObject> {
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

		public static readonly TObject BooleanTrue = new TObject(TType.BooleanType, true);
		public static readonly TObject BooleanFalse = new TObject(TType.BooleanType, false);
		public static readonly TObject BooleanNull = new TObject(TType.BooleanType, null);

		/// <summary>
		/// Returns a TObject of NULL type that represents a null value.
		/// </summary>
		public static readonly TObject Null = new TObject(TType.NullType, null);

		/// <summary>
		/// Returns a TObject of boolean type that is either true or false.
		/// </summary>
		/// <param name="b"></param>
		/// <returns></returns>
		public static TObject CreateBoolean(bool b) {
			return b ? BooleanTrue : BooleanFalse;
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given int value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject CreateInt4(int val) {
			return CreateBigNumber((BigNumber)val);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given long value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject CreateInt8(long val) {
			return CreateBigNumber((BigNumber)val);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given double value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject CreateDouble(double val) {
			return CreateBigNumber((BigNumber)val);
		}

		/// <summary>
		/// Returns a TObject of numeric type that represents the given BigNumber value.
		/// </summary>
		/// <param name="val"></param>
		/// <returns></returns>
		public static TObject CreateBigNumber(BigNumber val) {
			return new TObject(TType.NumericType, val);
		}

		/// <summary>
		/// Returns a TObject of VARCHAR type that represents the given 
		/// <see cref="StringObject"/> value.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static TObject CreateString(StringObject str) {
			return new TObject(TType.StringType, str);
		}

		/// <summary>
		/// Returns a TObject of VARCHAR type that represents the given 
		/// <see cref="string"/> value.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static TObject CreateString(String str) {
			return new TObject(TType.StringType, StringObject.FromString(str));
		}

		/// <summary>
		/// Returns a TObject of DATE type that represents the given time value.
		/// </summary>
		/// <param name="d"></param>
		/// <returns></returns>
		public static TObject CreateDateTime(DateTime d) {
			return new TObject(TType.DateType, d);
		}

		/// <summary>
		/// Returns a <see cref="TObject"/> of INTERVAL DAY TO SECOND type that 
		/// represents the given interval of time.
		/// </summary>
		/// <param name="t"></param>
		/// <returns></returns>
		public static TObject CreateInterval(TimeSpan t) {
			return new TObject(TType.GetIntervalType(SqlType.DayToSecond), new Interval(t.Days, t.Hours, t.Minutes, t.Seconds));
		}

		public static TObject CreateInterval(Interval i) {
			return new TObject(TType.IntervalType, i);
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="queryPlan"></param>
		/// <returns></returns>
		public static TObject CreateQueryPlan(IQueryPlanNode queryPlan) {
			return new TObject(TType.QueryPlanType, queryPlan);
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
				return CreateBigNumber((BigNumber)ob);
			if (ob is byte)
				return CreateBigNumber((byte) ob);
			if (ob is int)
				return CreateBigNumber((int)ob);
			if (ob is long)
				return CreateBigNumber((long) ob);
			if (ob is float)
				return CreateBigNumber((float) ob);
			if (ob is double)
				return CreateBigNumber((double) ob);
			if (ob is StringObject)
				return CreateString((StringObject)ob);
			if (ob is string)
				return CreateString(StringObject.FromString((string) ob));
			if (ob is bool)
				return CreateBoolean((Boolean)ob);
			if (ob is DateTime)
				return CreateDateTime((DateTime)ob);
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
				throw new NotSupportedException("Cannot compare other than TObject instances (for the moment).");
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
		/// <param name="destinationType"></param>
		/// <returns></returns>
		public TObject CastTo(TType destinationType) {
			return CreateAndCastFromObject(destinationType, Object);
		}


		/// <inheritdoc/>
		public override string ToString() {
			return IsNull ? "NULL" : Object.ToString();
		}

		void IDeserializationCallback.OnDeserialization(object sender) {
			if (ob is string)
				ob = StringObject.FromString((string) ob);
		}
	}
}