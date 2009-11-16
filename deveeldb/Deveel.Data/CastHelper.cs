//  
//  CastHelper.cs
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
using System.Text;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// Various utility methods for helping to cast an object to a 
	/// type that is conformant to an SQL type.
	/// </summary>
	public class CastHelper {
		private static BigNumber BD_ZERO = 0L;
		private static BigNumber BD_ONE = 1L;

		private static readonly string[] date_format_sql;
		private static readonly string[] time_format_sql;
		private static readonly string[] ts_format_sql;

		static CastHelper() {
			// The SQL time/date formatters
			date_format_sql = new string[1];
			date_format_sql[0] = "yyyy-MM-dd";

			//TODO: check this format on .NET ...
			time_format_sql = new string[4];
			time_format_sql[0] = "HH:mm:ss.fff z";
			time_format_sql[1] = "HH:mm:ss.fff";
			time_format_sql[2] = "HH:mm:ss z";
			time_format_sql[3] = "HH:mm:ss";

			ts_format_sql = new string[4];
			ts_format_sql[0] = "yyyy-MM-dd HH:mm:ss.fff z";
			ts_format_sql[1] = "yyyy-MM-dd HH:mm:ss.fff";
			ts_format_sql[2] = "yyyy-MM-dd HH:mm:ss z";
			ts_format_sql[3] = "yyyy-MM-dd HH:mm:ss";
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
			lock (ts_format_sql) {
				// ISSUE: We have to assume the date is a time stamp because we don't
				//   know if the date object represents an SQL DATE, TIMESTAMP or TIME.
				return d.ToString(ts_format_sql[1], CultureInfo.InvariantCulture);
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
			} else if (dif < 0) {
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
				return BD_ZERO;
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
		public static DateTime ToDate(String str) {
			lock (date_format_sql) {
				try {
					return DateTime.ParseExact(str, date_format_sql, CultureInfo.InvariantCulture, DateTimeStyles.None);
				} catch (FormatException) {
					throw new Exception(DateErrorString("Unable to parse string as a date ", date_format_sql));
				}
			}
		}

		/// <summary>
		/// Parses a String as an SQL time.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToTime(String str) {
			lock (time_format_sql) {
				try {
					return DateTime.ParseExact(str, time_format_sql, CultureInfo.InvariantCulture, DateTimeStyles.None);
				} catch(FormatException) {
					throw new Exception(DateErrorString("Unable to parse string as a time ", time_format_sql));
				}
			}
		}

		/// <summary>
		/// Parses a String as an SQL timestamp.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToTimeStamp(String str) {
			lock (ts_format_sql) {
				try {
					return DateTime.ParseExact(str, ts_format_sql, CultureInfo.InvariantCulture, DateTimeStyles.None);
				} catch (FormatException) {
					throw new Exception(DateErrorString("Unable to parse string as a timestamp ", ts_format_sql));
				}
			}
		}


		///<summary>
		/// Casts an object to the SQL type specified by the 
		/// given <see cref="DataTableColumnDef"/> object.
		///</summary>
		///<param name="ob">The <see cref="Object"/> to cast to the given type.</param>
		///<param name="sql_type">The destination <see cref="SqlType">SQL type</see>.</param>
		///<param name="sql_size">The size of the destination type.</param>
		///<param name="sql_scale">The scale of the destination type.</param>
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
		public static object CastToSQLType(object ob, SqlType sql_type, int sql_size, int sql_scale) {

			// If the input object is a ByteLongObject and the output type is not a
			// binary SQL type then we need to attempt to deserialize the object.
			if (ob is ByteLongObject) {
				if (sql_type != SqlType.Object &&
					 sql_type != SqlType.Blob &&
					 sql_type != SqlType.Binary &&
					 sql_type != SqlType.VarBinary &&
					 sql_type != SqlType.LongVarBinary) {
					// Attempt to deserialize it
					try {
						ob = ObjectTranslator.Deserialize((ByteLongObject)ob);
					} catch (Exception) {
						// Couldn't deserialize so it must be a standard blob which means
						// we are in error.
						throw new ApplicationException("Can't cast a BLOB to " + sql_type.ToString().ToUpper());
					}
				} else {
					// This is a ByteLongObject that is being cast to a binary type so
					// no further processing is necessary.
					return ob;
				}
			}

			// IBlobRef can be BINARY, OBJECT, VARBINARY or LONGVARBINARY
			if (ob is IBlobRef) {
				if (sql_type == SqlType.Binary ||
					sql_type == SqlType.Blob ||
					sql_type == SqlType.Object ||
					sql_type == SqlType.VarBinary ||
					sql_type == SqlType.LongVarBinary) {
					return ob;
				}
			}

			// IClobRef can be VARCHAR, LONGVARCHAR, or CLOB
			if (ob is IClobRef) {
				if (sql_type == SqlType.VarChar ||
					sql_type == SqlType.LongVarChar ||
					sql_type == SqlType.Clob) {
					return ob;
				}
			}

			// Cast from NULL
			if (ob == null) {
				switch (sql_type) {
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
						throw new ApplicationException("Can't cast NULL to " + sql_type.ToString().ToUpper());
				}
			}

			// Cast from a number
			if (ob is Number) {
				Number n = (Number)ob;
				switch (sql_type) {
					case (SqlType.Bit):
						return n.ToInt32() == 0 ? false : true;
					case (SqlType.TinyInt):
					// fall through
					case (SqlType.SmallInt):
					// fall through
					case (SqlType.Integer):
						//          return new BigDecimal(n.intValue());
						return (BigNumber)n.ToInt32();
					case (SqlType.BigInt):
						//          return new BigDecimal(n.longValue());
						return (BigNumber)n.ToInt64();
					case (SqlType.Float):
						return BigNumber.Parse(Convert.ToString(n.ToDouble()));
					case (SqlType.Real):
						return BigNumber.Parse(n.ToString());
					case (SqlType.Double):
						return BigNumber.Parse(Convert.ToString(n.ToDouble()));
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
						return BigNumber.Parse(n.ToString());
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(n.ToString(), sql_size));
					case (SqlType.VarChar):
						return StringObject.FromString(n.ToString());
					case (SqlType.LongVarChar):
						return StringObject.FromString(n.ToString());
					case (SqlType.Date):
						return ToDate(n.ToInt64());
					case (SqlType.Time):
						return ToDate(n.ToInt64());
					case (SqlType.TimeStamp):
						return ToDate(n.ToInt64());
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
					case (SqlType.Boolean):
						return n.ToInt32() == 0 ? false : true;
					default:
						throw new ApplicationException("Can't cast number to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Number)

			// Cast from a number
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


			// Cast from a string
			if (ob is StringObject || ob is String) {
				String str = ob.ToString();
				switch (sql_type) {
					case (SqlType.Bit):
						return String.Compare(str, "true", true) == 0 ? true : false;
					case (SqlType.TinyInt):
					// fall through
					case (SqlType.SmallInt):
					// fall through
					case (SqlType.Integer):
						//          return new BigDecimal(toBigDecimal(str).intValue());
						return (BigNumber)ToBigNumber(str).ToInt32();
					case (SqlType.BigInt):
						//          return new BigDecimal(toBigDecimal(str).longValue());
						return (BigNumber)ToBigNumber(str).ToInt64();
					case (SqlType.Float):
						return BigNumber.Parse(
									  Convert.ToString(ToBigNumber(str).ToDouble()));
					case (SqlType.Real):
						return ToBigNumber(str);
					case (SqlType.Double):
						return BigNumber.Parse(
									  Convert.ToString(ToBigNumber(str).ToDouble()));
					case (SqlType.Numeric):
					// fall through
					case (SqlType.Decimal):
						return ToBigNumber(str);
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(str, sql_size));
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
					case (SqlType.Boolean):
						return String.Compare(str, "true", true) == 0 ? true : false;
					case (SqlType.Clob):
						return StringObject.FromString(str);
					default:
						throw new ApplicationException("Can't cast string to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is String)

			// Cast from a boolean
			if (ob is Boolean) {
				Boolean b = (Boolean)ob;
				switch (sql_type) {
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
						return b ? BD_ONE : BD_ZERO;
					case (SqlType.Char):
						return StringObject.FromString(PaddedString(b.ToString(), sql_size));
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
						throw new ApplicationException("Can't cast boolean to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Boolean)

			// Cast from a date
			if (ob is DateTime) {
				DateTime d = (DateTime)ob;
				switch (sql_type) {
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
						return StringObject.FromString(PaddedString(FormatDateAsString(d), sql_size));
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
						throw new ApplicationException("Can't cast date to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Date)

			// Some obscure types
			if (ob is byte[]) {
				switch (sql_type) {
					case (SqlType.Blob):
					// fall through
					case (SqlType.Binary):
					// fall through
					case (SqlType.VarBinary):
					// fall through
					case (SqlType.LongVarBinary):
						return new ByteLongObject((byte[])ob);
					default:
						throw new ApplicationException("Can't cast byte[] to " + sql_type.ToString().ToUpper());
				}
			}

			// Finally, the object can only be something that we can cast to a
			// OBJECT.
			if (sql_type == SqlType.Object) {
				return ToObject(ob);
			}

			throw new Exception("Can't cast object " + ob.GetType() + " to " + sql_type.ToString().ToUpper());

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