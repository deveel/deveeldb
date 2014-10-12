// 
//  Copyright 2010-2014 Deveel
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

		public StringType(SqlTypeCode sqlType) 
			: this(sqlType, DefaultMaxSize) {
		}

		public StringType(SqlTypeCode sqlType, int maxSize) 
			: base("STRING", sqlType) {
			AssertIsString(sqlType);
			MaxSize = maxSize;
		}

		private static void AssertIsString(SqlTypeCode sqlType) {
			if (sqlType != SqlTypeCode.String &&
				sqlType != SqlTypeCode.VarChar &&
				sqlType != SqlTypeCode.Char &&
				sqlType != SqlTypeCode.LongVarChar &&
				sqlType != SqlTypeCode.Clob)
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

		private static NumericObject ToNumber(String str) {
			try {
				return NumericObject.Parse(str);
			} catch (Exception) {
				return NumericObject.Zero;
			}
		}


		/// <summary>
		/// Parses a String as an SQL date.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateObject ToDate(string str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.DateFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.Date, DateType.DateFormatSql));

			return result;
		}

		/// <summary>
		/// Parses a String as an SQL time.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateObject ToTime(String str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.TimeFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.NoCurrentDateDefault, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.Time, DateType.TimeFormatSql));

			return result;

		}

		/// <summary>
		/// Parses a String as an SQL timestamp.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		public static DateObject ToTimeStamp(String str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.TsFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.TimeStamp, DateType.TsFormatSql));

			return result;
		}

		private static string DateErrorMessage(string str, SqlTypeCode sqlType, string[] formats) {
			return String.Format("The input string {0} is not compatible with any of the formats for SQL Type {1} ( {2} )",
				str,
				sqlType.ToString().ToUpperInvariant(),
				String.Join(", ", formats));
		}


		public override DataObject CastTo(DataObject value, DataType destType) {
			string str = value.ToString();
			var sqlType = destType.SqlType;

			switch (sqlType) {
				case (SqlTypeCode.Bit):
				case (SqlTypeCode.Boolean):
					return (BooleanObject) (String.Compare(str, "true", StringComparison.OrdinalIgnoreCase) == 0 ||
					                        String.Compare(str, "1", StringComparison.OrdinalIgnoreCase) == 0);
				case (SqlTypeCode.TinyInt):
				// fall through
				case (SqlTypeCode.SmallInt):
				// fall through
				case (SqlTypeCode.Integer):
					return (NumericObject)ToNumber(str).ToInt32();
				case (SqlTypeCode.BigInt):
					return (NumericObject)ToNumber(str).ToInt64();
				case (SqlTypeCode.Float):
					return NumericObject.Parse(Convert.ToString(ToNumber(str).ToDouble()));
				case (SqlTypeCode.Real):
					return ToNumber(str);
				case (SqlTypeCode.Double):
					return NumericObject.Parse(Convert.ToString(ToNumber(str).ToDouble()));
				case (SqlTypeCode.Numeric):
				// fall through
				case (SqlTypeCode.Decimal):
					return ToNumber(str);
				case (SqlTypeCode.Char):
					return new StringObject(CastUtil.PaddedString(str, ((StringType)destType).MaxSize));
				case (SqlTypeCode.VarChar):
				case (SqlTypeCode.LongVarChar):
				case (SqlTypeCode.String):
					return new StringObject(str);
				case (SqlTypeCode.Date):
					return ToDate(str);
				case (SqlTypeCode.Time):
					return ToTime(str);
				case (SqlTypeCode.TimeStamp):
					return ToTimeStamp(str);
				case (SqlTypeCode.Blob):
				case (SqlTypeCode.Binary):
				case (SqlTypeCode.VarBinary):
				case (SqlTypeCode.LongVarBinary):
					return new BinaryObject(PrimitiveTypes.Binary(sqlType), Encoding.Unicode.GetBytes(str));
				case (SqlTypeCode.Null):
					return null;
				case (SqlTypeCode.Clob):
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
				return LexicographicalOrder((IStringAccessor)x, (IStringAccessor)y);

			return Collator.Compare(x.ToString(), y.ToString());
		}

		private static int LexicographicalOrder(IStringAccessor str1, IStringAccessor str2) {
			// If both strings are small use the 'toString' method to compare the
			// strings.  This saves the overhead of having to store very large string
			// objects in memory for all comparisons.
			long str1Size = str1.Length;
			long str2Size = str2.Length;
			if (str1Size < 32 * 1024 &&
				str2Size < 32 * 1024) {
				return String.Compare(str1.ToString(), str2.ToString(), StringComparison.Ordinal);
			}

			// The minimum size
			long size = System.Math.Min(str1Size, str2Size);
			TextReader r1 = str1.GetTextReader();
			TextReader r2 = str2.GetTextReader();
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