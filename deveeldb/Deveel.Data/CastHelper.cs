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
		private static BigNumber BD_ZERO = BigNumber.fromLong(0);
		private static BigNumber BD_ONE = BigNumber.fromLong(1);

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
				return BigNumber.fromString(str);
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
		///<param name="sql_type">The destination <see cref="SQLTypes">SQL type</see>.</param>
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
		public static object CastToSQLType(object ob, SQLTypes sql_type, int sql_size, int sql_scale) {

			// If the input object is a ByteLongObject and the output type is not a
			// binary SQL type then we need to attempt to deserialize the object.
			if (ob is ByteLongObject) {
				if (sql_type != SQLTypes.OBJECT &&
					 sql_type != SQLTypes.BLOB &&
					 sql_type != SQLTypes.BINARY &&
					 sql_type != SQLTypes.VARBINARY &&
					 sql_type != SQLTypes.LONGVARBINARY) {
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
				if (sql_type == SQLTypes.BINARY ||
					sql_type == SQLTypes.BLOB ||
					sql_type == SQLTypes.OBJECT ||
					sql_type == SQLTypes.VARBINARY ||
					sql_type == SQLTypes.LONGVARBINARY) {
					return ob;
				}
			}

			// IClobRef can be VARCHAR, LONGVARCHAR, or CLOB
			if (ob is IClobRef) {
				if (sql_type == SQLTypes.VARCHAR ||
					sql_type == SQLTypes.LONGVARCHAR ||
					sql_type == SQLTypes.CLOB) {
					return ob;
				}
			}

			// Cast from NULL
			if (ob == null) {
				switch (sql_type) {
					case (SQLTypes.BIT):
					// fall through
					case (SQLTypes.TINYINT):
					// fall through
					case (SQLTypes.SMALLINT):
					// fall through
					case (SQLTypes.INTEGER):
					// fall through
					case (SQLTypes.BIGINT):
					// fall through
					case (SQLTypes.FLOAT):
					// fall through
					case (SQLTypes.REAL):
					// fall through
					case (SQLTypes.DOUBLE):
					// fall through
					case (SQLTypes.NUMERIC):
					// fall through
					case (SQLTypes.DECIMAL):
					// fall through
					case (SQLTypes.CHAR):
					// fall through
					case (SQLTypes.VARCHAR):
					// fall through
					case (SQLTypes.LONGVARCHAR):
					// fall through
					case (SQLTypes.CLOB):
					// fall through
					case (SQLTypes.DATE):
					// fall through
					case (SQLTypes.TIME):
					// fall through
					case (SQLTypes.TIMESTAMP):
					// fall through
					case (SQLTypes.NULL):
					// fall through

					case (SQLTypes.BINARY):
					// fall through
					case (SQLTypes.VARBINARY):
					// fall through
					case (SQLTypes.LONGVARBINARY):
					// fall through
					case (SQLTypes.BLOB):
					// fall through


					case (SQLTypes.OBJECT):
					// fall through

					case (SQLTypes.BOOLEAN):
						return null;
					default:
						throw new ApplicationException("Can't cast NULL to " + sql_type.ToString().ToUpper());
				}
			}

			// Cast from a number
			if (ob is Number) {
				Number n = (Number)ob;
				switch (sql_type) {
					case (SQLTypes.BIT):
						return n.ToInt32() == 0 ? false : true;
					case (SQLTypes.TINYINT):
					// fall through
					case (SQLTypes.SMALLINT):
					// fall through
					case (SQLTypes.INTEGER):
						//          return new BigDecimal(n.intValue());
						return BigNumber.fromLong(n.ToInt32());
					case (SQLTypes.BIGINT):
						//          return new BigDecimal(n.longValue());
						return BigNumber.fromLong(n.ToInt64());
					case (SQLTypes.FLOAT):
						return BigNumber.fromString(Convert.ToString(n.ToDouble()));
					case (SQLTypes.REAL):
						return BigNumber.fromString(n.ToString());
					case (SQLTypes.DOUBLE):
						return BigNumber.fromString(Convert.ToString(n.ToDouble()));
					case (SQLTypes.NUMERIC):
					// fall through
					case (SQLTypes.DECIMAL):
						return BigNumber.fromString(n.ToString());
					case (SQLTypes.CHAR):
						return StringObject.FromString(PaddedString(n.ToString(), sql_size));
					case (SQLTypes.VARCHAR):
						return StringObject.FromString(n.ToString());
					case (SQLTypes.LONGVARCHAR):
						return StringObject.FromString(n.ToString());
					case (SQLTypes.DATE):
						return ToDate(n.ToInt64());
					case (SQLTypes.TIME):
						return ToDate(n.ToInt64());
					case (SQLTypes.TIMESTAMP):
						return ToDate(n.ToInt64());
					case (SQLTypes.BLOB):
					// fall through
					case (SQLTypes.BINARY):
					// fall through
					case (SQLTypes.VARBINARY):
					// fall through
					case (SQLTypes.LONGVARBINARY):
						return new ByteLongObject(Encoding.Unicode.GetBytes(n.ToString()));
					case (SQLTypes.NULL):
						return null;
					case (SQLTypes.OBJECT):
						return ToObject(ob);
					case (SQLTypes.BOOLEAN):
						return n.ToInt32() == 0 ? false : true;
					default:
						throw new ApplicationException("Can't cast number to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Number)

			// Cast from a number
			if (IsNumber(ob)) {
				switch (sql_type) {
					case (SQLTypes.BIT):
						return Convert.ToInt32(ob) == 0 ? false : true;
					case (SQLTypes.TINYINT):
					// fall through
					case (SQLTypes.SMALLINT):
					// fall through
					case (SQLTypes.INTEGER):
			//          return new BigDecimal(n.intValue());
						return BigNumber.fromLong(Convert.ToInt32(ob));
					case (SQLTypes.BIGINT):
						//          return new BigDecimal(n.longValue());
						return BigNumber.fromLong(Convert.ToInt64(ob));
					case (SQLTypes.FLOAT):
						return BigNumber.fromString(Convert.ToString(Convert.ToDouble(ob)));
					case (SQLTypes.REAL):
						return BigNumber.fromString(Convert.ToString(ob));
					case (SQLTypes.DOUBLE):
						return BigNumber.fromString(Convert.ToString(Convert.ToDouble(ob)));
					case (SQLTypes.NUMERIC):
					// fall through
					case (SQLTypes.DECIMAL):
						return BigNumber.fromString(ob.ToString());
					case (SQLTypes.CHAR):
						return StringObject.FromString(PaddedString(ob.ToString(), sql_size));
					case (SQLTypes.VARCHAR):
						return StringObject.FromString(ob.ToString());
					case (SQLTypes.LONGVARCHAR):
						return StringObject.FromString(ob.ToString());
					case (SQLTypes.DATE):
						return ToDate(Convert.ToInt64(ob));
					case (SQLTypes.TIME):
						return ToDate(Convert.ToInt64(ob));
					case (SQLTypes.TIMESTAMP):
						return ToDate(Convert.ToInt64(ob));
					case (SQLTypes.BLOB):
					// fall through
					case (SQLTypes.BINARY):
					// fall through
					case (SQLTypes.VARBINARY):
					// fall through
					case (SQLTypes.LONGVARBINARY):
						return new ByteLongObject(Encoding.Unicode.GetBytes(ob.ToString()));
					case (SQLTypes.NULL):
						return null;
					case (SQLTypes.OBJECT):
						return ToObject(ob);
					case (SQLTypes.BOOLEAN):
						return Convert.ToInt32(ob) == 0 ? false : true;
					default:
						throw new ApplicationException("Can't cast number to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Number)


			// Cast from a string
			if (ob is StringObject || ob is String) {
				String str = ob.ToString();
				switch (sql_type) {
					case (SQLTypes.BIT):
						return String.Compare(str, "true", true) == 0 ? true : false;
					case (SQLTypes.TINYINT):
					// fall through
					case (SQLTypes.SMALLINT):
					// fall through
					case (SQLTypes.INTEGER):
						//          return new BigDecimal(toBigDecimal(str).intValue());
						return BigNumber.fromLong(ToBigNumber(str).ToInt32());
					case (SQLTypes.BIGINT):
						//          return new BigDecimal(toBigDecimal(str).longValue());
						return BigNumber.fromLong(ToBigNumber(str).ToInt64());
					case (SQLTypes.FLOAT):
						return BigNumber.fromString(
									  Convert.ToString(ToBigNumber(str).ToDouble()));
					case (SQLTypes.REAL):
						return ToBigNumber(str);
					case (SQLTypes.DOUBLE):
						return BigNumber.fromString(
									  Convert.ToString(ToBigNumber(str).ToDouble()));
					case (SQLTypes.NUMERIC):
					// fall through
					case (SQLTypes.DECIMAL):
						return ToBigNumber(str);
					case (SQLTypes.CHAR):
						return StringObject.FromString(PaddedString(str, sql_size));
					case (SQLTypes.VARCHAR):
						return StringObject.FromString(str);
					case (SQLTypes.LONGVARCHAR):
						return StringObject.FromString(str);
					case (SQLTypes.DATE):
						return ToDate(str);
					case (SQLTypes.TIME):
						return ToTime(str);
					case (SQLTypes.TIMESTAMP):
						return ToTimeStamp(str);
					case (SQLTypes.BLOB):
					// fall through
					case (SQLTypes.BINARY):
					// fall through
					case (SQLTypes.VARBINARY):
					// fall through
					case (SQLTypes.LONGVARBINARY):
						return new ByteLongObject(Encoding.Unicode.GetBytes(str));
					case (SQLTypes.NULL):
						return null;
					case (SQLTypes.OBJECT):
						return ToObject(str);
					case (SQLTypes.BOOLEAN):
						return String.Compare(str, "true", true) == 0 ? true : false;
					case (SQLTypes.CLOB):
						return StringObject.FromString(str);
					default:
						throw new ApplicationException("Can't cast string to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is String)

			// Cast from a boolean
			if (ob is Boolean) {
				Boolean b = (Boolean)ob;
				switch (sql_type) {
					case (SQLTypes.BIT):
						return b;
					case (SQLTypes.TINYINT):
					// fall through
					case (SQLTypes.SMALLINT):
					// fall through
					case (SQLTypes.INTEGER):
					// fall through
					case (SQLTypes.BIGINT):
					// fall through
					case (SQLTypes.FLOAT):
					// fall through
					case (SQLTypes.REAL):
					// fall through
					case (SQLTypes.DOUBLE):
					// fall through
					case (SQLTypes.NUMERIC):
					// fall through
					case (SQLTypes.DECIMAL):
						return b ? BD_ONE : BD_ZERO;
					case (SQLTypes.CHAR):
						return StringObject.FromString(PaddedString(b.ToString(), sql_size));
					case (SQLTypes.VARCHAR):
						return StringObject.FromString(b.ToString());
					case (SQLTypes.LONGVARCHAR):
						return StringObject.FromString(b.ToString());
					case (SQLTypes.NULL):
						return null;
					case (SQLTypes.OBJECT):
						return ToObject(ob);
					case (SQLTypes.BOOLEAN):
						return b;
					default:
						throw new ApplicationException("Can't cast boolean to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Boolean)

			// Cast from a date
			if (ob is DateTime) {
				DateTime d = (DateTime)ob;
				switch (sql_type) {
					case (SQLTypes.TINYINT):
					// fall through
					case (SQLTypes.SMALLINT):
					// fall through
					case (SQLTypes.INTEGER):
					// fall through
					case (SQLTypes.BIGINT):
					// fall through
					case (SQLTypes.FLOAT):
					// fall through
					case (SQLTypes.REAL):
					// fall through
					case (SQLTypes.DOUBLE):
					// fall through
					case (SQLTypes.NUMERIC):
					// fall through
					case (SQLTypes.DECIMAL):
						return BigNumber.fromLong(d.Ticks);
					case (SQLTypes.CHAR):
						return StringObject.FromString(PaddedString(FormatDateAsString(d), sql_size));
					case (SQLTypes.VARCHAR):
						return StringObject.FromString(FormatDateAsString(d));
					case (SQLTypes.LONGVARCHAR):
						return StringObject.FromString(FormatDateAsString(d));
					case (SQLTypes.DATE):
						return d;
					case (SQLTypes.TIME):
						return d;
					case (SQLTypes.TIMESTAMP):
						return d;
					case (SQLTypes.NULL):
						return null;
					case (SQLTypes.OBJECT):
						return ToObject(ob);
					default:
						throw new ApplicationException("Can't cast date to " + sql_type.ToString().ToUpper());
				}
			}  // if (ob is Date)

			// Some obscure types
			if (ob is byte[]) {
				switch (sql_type) {
					case (SQLTypes.BLOB):
					// fall through
					case (SQLTypes.BINARY):
					// fall through
					case (SQLTypes.VARBINARY):
					// fall through
					case (SQLTypes.LONGVARBINARY):
						return new ByteLongObject((byte[])ob);
					default:
						throw new ApplicationException("Can't cast byte[] to " + sql_type.ToString().ToUpper());
				}
			}

			// Finally, the object can only be something that we can cast to a
			// OBJECT.
			if (sql_type == SQLTypes.OBJECT) {
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