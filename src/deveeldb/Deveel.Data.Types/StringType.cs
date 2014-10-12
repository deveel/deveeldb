// 
//  Copyright 2014  Deveel
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
using System.IO;
using System.Text;

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class StringType : DataType {
		private CompareInfo collator;

		public const int DefaultMaxSize = Int16.MaxValue;

		public StringType(SqlType sqlType) 
			: this(sqlType, DefaultMaxSize) {
		}

		public StringType(SqlType sqlType, int maxSize) 
			: base("STRING", sqlType) {
			AssertIsString(sqlType);
			MaxSize = maxSize;
		}

		private static void AssertIsString(SqlType sqlType) {
			if (sqlType != SqlType.String &&
				sqlType != SqlType.VarChar &&
				sqlType != SqlType.Char &&
				sqlType != SqlType.LongVarChar &&
				sqlType != SqlType.Clob)
				throw new ArgumentException(String.Format("The type {0} is not a valid STRING type.", sqlType), "sqlType");
		}

		public int MaxSize { get; private set; }

		public CultureInfo Locale { get; private set; }

		private CompareInfo Collator {
			get {
				lock (this) {
					if (collator != null) {
						return collator;
					} else {
						//TODO:
						collator = Locale.CompareInfo;
						return collator;
					}
				}
			}
		}

		public override string ToString() {
			var sb = new StringBuilder(Name);
			if (MaxSize >= 0)
				sb.AppendFormat("({0})", MaxSize);

			return sb.ToString();
		}

		public override bool IsComparable(DataType type) {
			// Are we comparing with another string type?
			if (!(type is StringType))
				return false;

			var stringType = (StringType)type;
			// If either locale is null return true
			if (Locale == null || stringType.Locale == null)
				return true;

			// If the locales are the same return true
			return Locale.Equals(stringType.Locale);
		}

		private static Number ToBigNumber(String str) {
			try {
				return Number.Parse(str);
			} catch (Exception) {
				return Number.Zero;
			}
		}


		/// <summary>
		/// Parses a String as an SQL date.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToDate(string str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.DateFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlType.Date, DateType.DateFormatSql));

			return result;
		}

		/// <summary>
		/// Parses a String as an SQL time.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToTime(String str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.TimeFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.NoCurrentDateDefault, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlType.Time, DateType.TimeFormatSql));

			return result;

		}

		/// <summary>
		/// Parses a String as an SQL timestamp.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateTime ToTimeStamp(String str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.TsFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlType.TimeStamp, DateType.TsFormatSql));

			return result;
		}

		private static string DateErrorMessage(string str, SqlType sqlType, string[] formats) {
			return String.Format("The input string {0} is not compatible with any of the formats for SQL Type {1} ( {2} )",
				str,
				sqlType.ToString().ToUpperInvariant(),
				String.Join(", ", formats));
		}


		public override DataObject CastTo(DataObject value, DataType destType) {
			string str = value.ToString();
			var sqlType = destType.SqlType;

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
					return (Number)ToBigNumber(str).ToInt32();
				case (SqlType.BigInt):
					return (Number)ToBigNumber(str).ToInt64();
				case (SqlType.Float):
					return Number.Parse(Convert.ToString(ToBigNumber(str).ToDouble()));
				case (SqlType.Real):
					return ToBigNumber(str);
				case (SqlType.Double):
					return Number.Parse(Convert.ToString(ToBigNumber(str).ToDouble()));
				case (SqlType.Numeric):
				// fall through
				case (SqlType.Decimal):
					return ToBigNumber(str);
				case (SqlType.Char):
					return new StringObject(CastUtil.PaddedString(str, ((StringType)destType).MaxSize));
				case (SqlType.VarChar):
				case (SqlType.LongVarChar):
				case (SqlType.String):
					return new StringObject(str);
				case (SqlType.Date):
					return ToDate(str);
				case (SqlType.Time):
					return ToTime(str);
				case (SqlType.TimeStamp):
					return ToTimeStamp(str);
				case (SqlType.Blob):
				case (SqlType.Binary):
				case (SqlType.VarBinary):
				case (SqlType.LongVarBinary):
					return new BinaryObject(Encoding.Unicode.GetBytes(str));
				case (SqlType.Null):
					return null;
				case (SqlType.Clob):
					// TODO: have a context where to get a new CLOB
					return new StringObject(str);
				default:
					throw new InvalidCastException();
			}
		}

		public override int Compare(DataObject x, DataObject y) {
			if (x == y)
				return 0;

			// If lexicographical ordering,
			if (Locale == null)
				return LexicographicalOrder((IStringObject)x, (IStringObject)y);

			return Collator.Compare(x.ToString(), y.ToString());
		}

		private static int LexicographicalOrder(IStringObject str1, IStringObject str2) {
			// If both strings are small use the 'toString' method to compare the
			// strings.  This saves the overhead of having to store very large string
			// objects in memory for all comparisons.
			long str1Size = str1.Length;
			long str2Size = str2.Length;
			if (str1Size < 32 * 1024 &&
				str2Size < 32 * 1024) {
				return str1.ToString().CompareTo(str2.ToString());
			}

			// The minimum size
			long size = System.Math.Min(str1Size, str2Size);
			TextReader r1 = str1.GetInput();
			TextReader r2 = str2.GetInput();
			try {
				try {
					while (size > 0) {
						int c1 = r1.Read();
						int c2 = r2.Read();
						if (c1 != c2) {
							return c1 - c2;
						}
						--size;
					}
					// They compare equally up to the limit, so now compare sizes,
					if (str1Size > str2Size) {
						// If str1 is larger
						return 1;
					} else if (str1Size < str2Size) {
						// If str1 is smaller
						return -1;
					}
					// Must be equal
					return 0;
				} finally {
					r1.Close();
					r2.Close();
				}
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}

		}
	}
}