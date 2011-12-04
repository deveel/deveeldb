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
using System.Globalization;

namespace Deveel.Data {
	public sealed partial class TObject {
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
				if (val.TType is TIntervalType) {
					Interval v2 = val.ToInterval();
					v1 = v1.AddYears(v2.Years)
						.AddMonths(v2.Months)
						.AddDays(v2.Days)
						.AddMinutes(v2.Minutes)
						.AddSeconds(v2.Seconds);
					return new TObject(TType.DateType, v1);
				}
			} else if (TType is TIntervalType) {
				Interval v1 = ToInterval();
				if (val.TType is TIntervalType) {
					Interval v2 = val.ToInterval();
					return new TObject(TType, v1.Add(v2));
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
				if (val.TType is TIntervalType) {
					Interval v2 = val.ToInterval();
					v1 = v1.AddYears(-v2.Years)
						.AddMonths(-v2.Months)
						.AddDays(-v2.Days)
						.AddMinutes(-v2.Minutes)
						.AddSeconds(-v2.Seconds);

					return new TObject(TType.DateType, v1);
				}
				if (val.TType is TDateType) {
					DateTime v2 = val.ToDateTime();
					return new TObject(TType.IntervalType, new Interval(v1, v2));
				}
			} else if (TType is TIntervalType) {
				Interval v1 = ToInterval();
				if (val.TType is TIntervalType) {
					Interval v2 = val.ToInterval();
					return new TObject(TType, v1.Add(v2));
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
			if (IsComparableTo(val))
				return CreateBoolean(CompareTo(val) == 0);
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
			if (IsComparableTo(val) && !IsNull && !val.IsNull) {
				return CreateBoolean(CompareToNoNulls(val) == 0);
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
			if (IsComparableTo(val) && !IsNull && !val.IsNull) {
				return CreateBoolean(CompareToNoNulls(val) != 0);
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
			if (IsComparableTo(val) && !IsNull && !val.IsNull) {
				return CreateBoolean(CompareToNoNulls(val) > 0);
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
			if (IsComparableTo(val) && !IsNull && !val.IsNull) {
				return CreateBoolean(CompareToNoNulls(val) >= 0);
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
			if (IsComparableTo(val) && !IsNull && !val.IsNull) {
				return CreateBoolean(CompareToNoNulls(val) < 0);
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
			if (IsComparableTo(val) && !IsNull && !val.IsNull) {
				return CreateBoolean(CompareToNoNulls(val) <= 0);
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
			return CreateBoolean(String.Compare(sd1, sd2, false) == 0);
		}


		/// <summary>
		/// Performs a logical NOT on this value.
		/// </summary>
		/// <returns>
		/// </returns>
		public TObject Not() {
			// If type is null
			if (IsNull)
				return this;

			bool? b = ToNullableBoolean();
			return b.HasValue ? CreateBoolean(!b.Value) : BooleanNull;
		}

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

	}
}