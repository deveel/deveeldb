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
using System.Globalization;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Math;

namespace Deveel.Data.Types {
	/// <summary>
	/// Various utility methods for helping to cast an object to a 
	/// type that is conformant to an SQL type.
	/// </summary>
	public static class CastHelper {
		private static readonly BigNumber BdZero = 0L;
		private static readonly BigNumber BdOne = 1L;

		private static readonly string[] DateFormatSql;
		private static readonly string[] TimeFormatSql;
		private static readonly string[] TsFormatSql;

		static CastHelper() {
			// The SQL time/date formatters
			DateFormatSql = new[] {
				"yyyy-MM-dd",
				"yyyy MM dd"
			};

			//TODO: check this format on .NET ...
			TimeFormatSql = new[] {
				"HH:mm:ss.fff z",
				"HH:mm:ss.fff zz",
				"HH:mm:ss.fff zzz",
				"HH:mm:ss.fff", 
				"HH:mm:ss z", 
				"HH:mm:ss zz",
				"HH:mm:ss zzz", 
				"HH:mm:ss"
			};

			TsFormatSql = new[] {
				"yyyy-MM-dd HH:mm:ss.fff",
				"yyyy-MM-dd HH:mm:ss.fff z",
				"yyyy-MM-dd HH:mm:ss.fff zz",
				"yyyy-MM-dd HH:mm:ss.fff zzz",
				"yyyy-MM-dd HH:mm:ss",
				"yyyy-MM-dd HH:mm:ss z",
				"yyyy-MM-dd HH:mm:ss zz",
				"yyyy-MM-dd HH:mm:ss zzz",

				"yyyy-MM-ddTHH:mm:ss.fff",
				"yyyy-MM-ddTHH:mm:ss.fff z",
				"yyyy-MM-ddTHH:mm:ss.fff zz",
				"yyyy-MM-ddTHH:mm:ss.fff zzz",
				"yyyy-MM-ddTHH:mm:ss",
				"yyyy-MM-ddTHH:mm:ss z",
				"yyyy-MM-ddTHH:mm:ss zz",
				"yyyy-MM-ddTHH:mm:ss zzz",
			};
		}


		/// <summary>
		/// Converts the given object to an SQL OBJECT type by 
		/// serializing the object.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		private static Object ToObject(Object ob) {
			try {
				return ObjectTranslator.Serialize(ob);
			} catch (ExecutionEngineException) {
				throw new ApplicationException("Can't serialize object " + ob.GetType());
			}
		}

		/// <summary>
		/// Formats the date object as a standard SQL string.
		/// </summary>
		/// <param name="d"></param>
		/// <returns></returns>
		private static String FormatDateAsString(DateTime d) {
			lock (TsFormatSql) {
				// ISSUE: We have to assume the date is a time stamp because we don't
				//   know if the date object represents an SQL DATE, TIMESTAMP or TIME.
				return d.ToString(TsFormatSql[1], CultureInfo.InvariantCulture);
			}
		}

		/// <summary>
		/// Returns the given string padded or truncated to the given size.
		/// </summary>
		/// <param name="str"></param>
		/// <param name="size"></param>
		/// <remarks>
		/// If size is -1 then the size doesn't matter.
		/// </remarks>
		/// <returns></returns>
		private static String PaddedString(String str, int size) {
			if (size == -1) {
				return str;
			}
			int dif = size - str.Length;
			if (dif > 0) {
				StringBuilder buf = new StringBuilder(str);
				for (int n = 0; n < dif; ++n) {
					buf.Append(' ');
				}
				return buf.ToString();
			}
			if (dif < 0) {
				return str.Substring(0, size);
			}
			return str;
		}

		/// <summary>
		/// Returns the given long value as a date object.
		/// </summary>
		/// <param name="time"></param>
		/// <returns></returns>
		private static DateTime ToDate(long time) {
			return new DateTime(time);
		}

		/// <summary>
		/// Converts the given string to a BigNumber or 0 if the cast fails.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		private static BigNumber ToBigNumber(String str) {
			try {
				return BigNumber.Parse(str);
			} catch (Exception) {
				return BdZero;
			}
		}

		/// <summary>
		/// Helper that generates an appropriate error message for a date format error.
		/// </summary>
		/// <param name="msg"></param>
		/// <param name="df"></param>
		/// <returns></returns>
		private static String DateErrorString(String msg, string[] df) {
			String pattern = "(" + df[0] + ")";
			return msg + pattern;
		}

		/// <summary>
		/// Parses a String as an SQL date.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToDate(string str) {
			lock (DateFormatSql) {
				DateTime result;
				if (!DateTime.TryParseExact(str, DateFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
					throw new FormatException(DateErrorString("Unable to parse string as a date ", DateFormatSql));

				return result;
			}
		}

		/// <summary>
		/// Parses a String as an SQL time.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToTime(String str) {
			lock (TimeFormatSql) {
				try {
					return DateTime.ParseExact(str, TimeFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.NoCurrentDateDefault);
				} catch(FormatException) {
					throw new Exception(DateErrorString("Unable to parse string as a time ", TimeFormatSql));
				}
			}
		}

		/// <summary>
		/// Parses a String as an SQL timestamp.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToTimeStamp(String str) {
			lock (TsFormatSql) {
				try {
					return DateTime.ParseExact(str, TsFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None);
				} catch (FormatException) {
					throw new Exception(DateErrorString("Unable to parse string as a timestamp ", TsFormatSql));
				}
			}
		}


		///<summary>
		/// Casts an object to the SQL type specified by the 
		/// given <see cref="DataColumnInfo"/> object.
		///</summary>
		///<param name="ob">The <see cref="Object"/> to cast to the given type.</param>
		///<param name="sqlType">The destination <see cref="SqlType">SQL type</see>.</param>
		///<param name="sqlSize">The size of the destination type.</param>
		///<param name="sqlScale">The scale of the destination type.</param>
		/// <remarks>
		/// This is used for the following engine functions:
		/// <list type="bullet">
		/// <item>To prepare a value for insertion into the data store.  For example,
		/// the table column may be <c>STRING</c> but the value here is a BigNumber.</item>
		/// <item>To cast an object to a specific type in an SQL function such as
		/// <c>CAST</c>.</item>
		/// </list>
		/// <para>
		/// Given any supported object, this will return the internal database
		/// representation of the object as either <see cref="BigNumber"/>, 
		/// <see cref="String"/>, <see cref="DateTime"/>, <see cref="Boolean"/> 
		/// or <see cref="ByteLongObject"/>.
		/// </para>
		/// </remarks>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		///<exception cref="Exception"></exception>
		public static object CastToSqlType(object ob, SqlType sqlType, int sqlSize, int sqlScale) {
			// If the input object is a ByteLongObject and the output type is not a
			// binary SQL type then we need to attempt to deserialize the object.
			if (ob is ByteLongObject) {
				if (sqlType != SqlType.Object &&
					 sqlType != SqlType.Blob &&
					 sqlType != SqlType.Binary &&
					 sqlType != SqlType.VarBinary &&
					 sqlType != SqlType.LongVarBinary) {
					// Attempt to deserialize it
					try {
						ob = ObjectTranslator.Deserialize((ByteLongObject)ob);
					} catch (Exception) {
						// Couldn't deserialize so it must be a standard blob which means
						// we are in error.
						throw new ApplicationException("Can't cast a BLOB to " + sqlType.ToString().ToUpper());
					}
				} else {
					// This is a ByteLongObject that is being cast to a binary type so
					// no further processing is necessary.
					return ob;
				}
			}

			// IBlobRef can be BINARY, OBJECT, VARBINARY or LONGVARBINARY
			if (ob is IBlobRef) {
				if (sqlType == SqlType.Binary ||
					sqlType == SqlType.Blob ||
					sqlType == SqlType.Object ||
					sqlType == SqlType.VarBinary ||
					sqlType == SqlType.LongVarBinary) {
					return ob;
				}
			}

			// IClobRef can be VARCHAR, LONGVARCHAR, or CLOB
			if (ob is IClobRef) {
				if (sqlType == SqlType.VarChar ||
					sqlType == SqlType.LongVarChar ||
					sqlType == SqlType.Clob) {
					return ob;
				}
			}

			// Cast from NULL
			if (ob == null) {
				switch (sqlType) {
					case (SqlType.Bit):
					// fall through
					case (SqlType.TinyInt):
					// fall through
					case (SqlType.SmallInt):
					// fall through
					case (SqlType.Integer):
					// fall through
					case (SqlType.BigInt):
					// fall through
					case (SqlType.Float):
					// fall through
					case (SqlType.Real):
					// fall through
					case (SqlType.Double):
					// fall through
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
					// fall through
					case (SqlType.Char):
					// fall through
					case (SqlType.VarChar):
					// fall through
					case (SqlType.LongVarChar):
					// fall through
					case (SqlType.Clob):
					// fall through
					case (SqlType.Date):
					// fall through
					case (SqlType.Time):
					// fall through
					case (SqlType.TimeStamp):
					// fall through
					case (SqlType.Null):
					// fall through

					case (SqlType.Binary):
					// fall through
					case (SqlType.VarBinary):
					// fall through
					case (SqlType.LongVarBinary):
					// fall through
					case (SqlType.Blob):
					// fall through


					case (SqlType.Object):
					// fall through

					case (SqlType.Boolean):
						return null;
					default:
						throw new ApplicationException("Can't cast NULL to " + sqlType.ToString().ToUpper());
				}
			}

			// Cast from a string
			if (ob is StringObject || ob is string) {
				String str = ob.ToString();
				switch (sqlType) {
					case (SqlType.Bit):
					case (SqlType.Boolean):
						return (String.Compare(str, "true", StringComparison.OrdinalIgnoreCase) == 0 ||
						        String.Compare(str, "1", StringComparison.OrdinalIgnoreCase) == 0);
					case (SqlType.TinyInt):
					// fall through
					case (SqlType.SmallInt):
					// fall through
					case (SqlType.Integer):
						return (BigNumber)ToBigNumber(str).ToInt32();
					case (SqlType.BigInt):
						return (BigNumber)ToBigNumber(str).ToInt64();
					case (SqlType.Float):
						return BigNumber.Parse(Convert.ToString(ToBigNumber(str).ToDouble()));
					case (SqlType.Real):
						return ToBigNumber(str);
					case (SqlType.Double):
						return BigNumber.Parse(Convert.ToString(ToBigNumber(str).ToDouble()));
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
						return ToBigNumber(str);
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(str, sqlSize));
					case (SqlType.VarChar):
						return StringObject.FromString(str);
					case (SqlType.LongVarChar):
						return StringObject.FromString(str);
					case (SqlType.Date):
						return ToDate(str);
					case (SqlType.Time):
						return ToTime(str);
					case (SqlType.TimeStamp):
						return ToTimeStamp(str);
					case (SqlType.Blob):
					// fall through
					case (SqlType.Binary):
					// fall through
					case (SqlType.VarBinary):
					// fall through
					case (SqlType.LongVarBinary):
						return new ByteLongObject(Encoding.Unicode.GetBytes(str));
					case (SqlType.Null):
						return null;
					case (SqlType.Object):
						return ToObject(str);
					case (SqlType.Clob):
						return StringObject.FromString(str);
					default:
						throw new ApplicationException("Can't cast string to " + sqlType.ToString().ToUpper());
				}
			}  // if (ob is String)

			// Cast from a convertible type (generally numbers)
			if (ob is IConvertible) {
				IConvertible n = (IConvertible)ob;
				switch (sqlType) {
					case (SqlType.Bit):
					case (SqlType.Boolean):
						return n.ToBoolean(null);
					case (SqlType.TinyInt):
					case (SqlType.SmallInt):
					case (SqlType.Integer):
						return (BigNumber)n.ToInt32(null);
					case SqlType.Identity:
					case (SqlType.BigInt):
						return (BigNumber)n.ToInt64(null);
					case (SqlType.Float):
					case (SqlType.Real):
					case (SqlType.Double):
						double d = n.ToDouble(null);
						NumberState state = NumberState.None;
						if (Double.IsNaN(d))
							state = NumberState.NotANumber;
						else if (Double.IsPositiveInfinity(d))
							state = NumberState.PositiveInfinity;
						else if (Double.IsNegativeInfinity(d))
							state = NumberState.NegativeInfinity;
						return new BigNumber(state, new BigDecimal(d));
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
						return BigNumber.Parse(n.ToString());
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(n.ToString(), sqlSize));
					case (SqlType.VarChar):
						return StringObject.FromString(n.ToString());
					case (SqlType.LongVarChar):
						return StringObject.FromString(n.ToString());
					case (SqlType.Date):
					case (SqlType.Time):
					case (SqlType.TimeStamp):
						return ToDate(n.ToInt64(null));
					case (SqlType.Blob):
					// fall through
					case (SqlType.Binary):
					// fall through
					case (SqlType.VarBinary):
					// fall through
					case (SqlType.LongVarBinary):
						return new ByteLongObject(Encoding.Unicode.GetBytes(n.ToString()));
					case (SqlType.Null):
						return null;
					case (SqlType.Object):
						return ToObject(ob);
					default:
						throw new ApplicationException("Can't cast number to " + sqlType.ToString().ToUpper());
				}
			}  // if (ob is Number)

			// Cast from a number
			/*
			if (IsNumber(ob)) {
				switch (sql_type) {
					case (SqlType.Bit):
						return Convert.ToInt32(ob) == 0 ? false : true;
					case (SqlType.TinyInt):
					// fall through
					case (SqlType.SmallInt):
					// fall through
					case (SqlType.Integer):
			//          return new BigDecimal(n.intValue());
						return (BigNumber)Convert.ToInt32(ob);
					case SqlType.Identity:
						// fall through
					case (SqlType.BigInt):
						//          return new BigDecimal(n.longValue());
						return (BigNumber)Convert.ToInt64(ob);
					case (SqlType.Float):
						return BigNumber.Parse(Convert.ToString(Convert.ToDouble(ob)));
					case (SqlType.Real):
						return BigNumber.Parse(Convert.ToString(ob));
					case (SqlType.Double):
						return BigNumber.Parse(Convert.ToString(Convert.ToDouble(ob)));
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
						return BigNumber.Parse(ob.ToString());
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(ob.ToString(), sql_size));
					case (SqlType.VarChar):
						return StringObject.FromString(ob.ToString());
					case (SqlType.LongVarChar):
						return StringObject.FromString(ob.ToString());
					case (SqlType.Date):
						return ToDate(Convert.ToInt64(ob));
					case (SqlType.Time):
						return ToDate(Convert.ToInt64(ob));
					case (SqlType.TimeStamp):
						return ToDate(Convert.ToInt64(ob));
					case (SqlType.Blob):
					// fall through
					case (SqlType.Binary):
					// fall through
					case (SqlType.VarBinary):
					// fall through
					case (SqlType.LongVarBinary):
						return new ByteLongObject(Encoding.Unicode.GetBytes(ob.ToString()));
					case (SqlType.Null):
						return null;
					case (SqlType.Object):
						return ToObject(ob);
					case (SqlType.Boolean):
						return Convert.ToInt32(ob) == 0 ? false : true;
					default:
						throw new ApplicationException("Can't cast number to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Number)
			*/


			// Cast from a boolean
			if (ob is Boolean) {
				Boolean b = (Boolean)ob;
				switch (sqlType) {
					case (SqlType.Bit):
						return b;
					case (SqlType.TinyInt):
					// fall through
					case (SqlType.SmallInt):
					// fall through
					case (SqlType.Integer):
					// fall through
					case (SqlType.BigInt):
					// fall through
					case (SqlType.Float):
					// fall through
					case (SqlType.Real):
					// fall through
					case (SqlType.Double):
					// fall through
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
						return b ? BdOne : BdZero;
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(b.ToString(), sqlSize));
					case (SqlType.VarChar):
						return StringObject.FromString(b.ToString());
					case (SqlType.LongVarChar):
						return StringObject.FromString(b.ToString());
					case (SqlType.Null):
						return null;
					case (SqlType.Object):
						return ToObject(ob);
					case (SqlType.Boolean):
						return b;
					default:
						throw new ApplicationException("Can't cast boolean to " + sqlType.ToString().ToUpper());
				}
			}  // if (ob is Boolean)

			// Cast from a date
			if (ob is DateTime) {
				DateTime d = (DateTime)ob;
				switch (sqlType) {
					case (SqlType.TinyInt):
					// fall through
					case (SqlType.SmallInt):
					// fall through
					case (SqlType.Integer):
					// fall through
					case (SqlType.BigInt):
					// fall through
					case (SqlType.Float):
					// fall through
					case (SqlType.Real):
					// fall through
					case (SqlType.Double):
					// fall through
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
						return (BigNumber)d.Ticks;
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(FormatDateAsString(d), sqlSize));
					case (SqlType.VarChar):
						return StringObject.FromString(FormatDateAsString(d));
					case (SqlType.LongVarChar):
						return StringObject.FromString(FormatDateAsString(d));
					case (SqlType.Date):
						return d;
					case (SqlType.Time):
						return d;
					case (SqlType.TimeStamp):
						return d;
					case (SqlType.Null):
						return null;
					case (SqlType.Object):
						return ToObject(ob);
					default:
						throw new ApplicationException("Can't cast date to " + sqlType.ToString().ToUpper());
				}
			}  // if (ob is Date)

			// Some obscure types
			if (ob is byte[]) {
				switch (sqlType) {
					case (SqlType.Blob):
					// fall through
					case (SqlType.Binary):
					// fall through
					case (SqlType.VarBinary):
					// fall through
					case (SqlType.LongVarBinary):
						return new ByteLongObject((byte[])ob);
					default:
						throw new ApplicationException("Can't cast byte[] to " + sqlType.ToString().ToUpper());
				}
			}

			// Finally, the object can only be something that we can cast to a
			// OBJECT.
			if (sqlType == SqlType.Object) {
				return ToObject(ob);
			}

			throw new Exception("Can't cast object " + ob.GetType() + " to " + sqlType.ToString().ToUpper());

		}

		private static bool IsNumber(object value) {
			if (value is sbyte) return true;
			if (value is byte) return true;
			if (value is short) return true;
			if (value is ushort) return true;
			if (value is int) return true;
			if (value is uint) return true;
			if (value is long) return true;
			if (value is ulong) return true;
			if (value is float) return true;
			if (value is double) return true;
			if (value is decimal) return true;
			return false;
		}

	}
}