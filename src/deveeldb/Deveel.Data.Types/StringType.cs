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

using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Types {
	[Serializable]
	public sealed class StringType : DataType {
		private CompareInfo collator;

		public const int DefaultMaxSize = Int16.MaxValue;

		public StringType(SqlTypeCode sqlType) 
			: this(sqlType, DefaultMaxSize) {
		}

		public StringType(SqlTypeCode sqlType, int maxSize) 
			: this(sqlType, maxSize, null) {
		}

		public StringType(SqlTypeCode sqlType, CultureInfo locale) 
			: this(sqlType, DefaultMaxSize, locale) {
		}

		public StringType(SqlTypeCode sqlType, int maxSize, CultureInfo locale) 
			: base("STRING", sqlType) {
			AssertIsString(sqlType);
			MaxSize = maxSize;
			Locale = locale;
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

		public Encoding Encoding {
			get {
				if (Locale == null)
					return Encoding.Unicode;

				return Encoding.GetEncoding(Locale.TextInfo.OEMCodePage);
			}
		}

		public override string ToString() {
			var sb = new StringBuilder(Name);
			if (MaxSize >= 0)
				sb.AppendFormat("({0})", MaxSize);

			return sb.ToString();
		}

		public override bool Equals(DataType other) {
			if (!base.Equals(other))
				return false;

			var stringType = (StringType) other;
			if (stringType.MaxSize != MaxSize)
				return false;

			if (Locale == null && stringType.Locale == null)
				return true;
			if (Locale == null && stringType.Locale != null)
				return false;
			if (Locale != null && stringType.Locale == null)
				return false;

			if (Locale != null && stringType.Locale != null)
				return Locale.NativeName.Equals(stringType.Locale.NativeName);

			return true;
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}

		/// <inheritdoc/>
		public override bool IsComparable(DataType type) {
			// Are we comparing with another string type?
			if (type is StringType) {
				var stringType = (StringType) type;
				// If either locale is null return true
				if (Locale == null || stringType.Locale == null)
					return true;

				//TODO: Check batter on the locale comparison: we could compare
				//      neutral cultures

				// If the locales are the same return true
				return Locale.Equals(stringType.Locale);
			}

			// Only string types can be comparable
			return false;
		}

		#region Operators

		/// <inheritdoc/>
		public override ISqlObject Add(ISqlObject a, ISqlObject b) {
			if (!(a is ISqlString))
				throw new ArgumentException();

			if (!(b is ISqlString)) {
				//TODO: convert to a ISqlString
				throw new NotSupportedException();
			}

			if (a is SqlString) {
				var x = (SqlString) a;
				var y = (ISqlString) b;
				return x.Concat(y);
			}

			return base.Add(a, b);
		}

		#endregion

		private static SqlNumber ToNumber(String str) {
			SqlNumber value;
			if (!SqlNumber.TryParse(str, out value))
				value = SqlNumber.Null;

			return value;
		}


		/// <summary>
		/// Parses a String as an SQL date.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		private static SqlDateTime ToDate(string str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.DateFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.Date, DateType.DateFormatSql));

			return new SqlDateTime(result.Ticks);
		}

		/// <summary>
		/// Parses a String as an SQL time.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		private static SqlDateTime ToTime(String str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.TimeFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.NoCurrentDateDefault, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.Time, DateType.TimeFormatSql));

			return new SqlDateTime(result.Ticks);

		}

		/// <summary>
		/// Parses a String as an SQL timestamp.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		private static SqlDateTime ToTimeStamp(String str) {
			DateTime result;
			if (!DateTime.TryParseExact(str, DateType.TsFormatSql, CultureInfo.InvariantCulture, DateTimeStyles.None, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.TimeStamp, DateType.TsFormatSql));

			return new SqlDateTime(result.Ticks);
		}

		private static string DateErrorMessage(string str, SqlTypeCode sqlType, string[] formats) {
			return String.Format("The input string {0} is not compatible with any of the formats for SQL Type {1} ( {2} )",
				str,
				sqlType.ToString().ToUpperInvariant(),
				String.Join(", ", formats));
		}


		public override DataObject CastTo(DataObject value, DataType destType) {
			string str = value.Value.ToString();
			var sqlType = destType.SqlType;
			ISqlObject castedValue;

			switch (sqlType) {
				case (SqlTypeCode.Bit):
				case (SqlTypeCode.Boolean):
					castedValue = (SqlBoolean) (String.Compare(str, "true", StringComparison.OrdinalIgnoreCase) == 0 ||
					                        String.Compare(str, "1", StringComparison.OrdinalIgnoreCase) == 0);
					break;
				case (SqlTypeCode.TinyInt):
				case (SqlTypeCode.SmallInt):
				case (SqlTypeCode.Integer): {
					var num = ToNumber(str);
					if (num.IsNull) {
						castedValue = num;
					} else {
						castedValue = new SqlNumber(num.ToInt32());
					}

					break;
				}
				case (SqlTypeCode.BigInt): {
					var num = ToNumber(str);
					if (num.IsNull) {
						castedValue = num;
					} else {
						castedValue = new SqlNumber(num.ToInt64());
					}

					break;
				}
				case (SqlTypeCode.Float):
				case (SqlTypeCode.Double): {
					var num = ToNumber(str);
					if (num.IsNull) {
						castedValue = num;
					} else {
						castedValue = new SqlNumber(num.ToDouble());
					}

					break;
				}
				case (SqlTypeCode.Real):
				case (SqlTypeCode.Numeric):
				case (SqlTypeCode.Decimal):
					castedValue = ToNumber(str);
					break;
				case (SqlTypeCode.Char):
					castedValue = new SqlString(str.PadRight(((StringType)destType).MaxSize));
					break;
				case (SqlTypeCode.VarChar):
				case (SqlTypeCode.LongVarChar):
				case (SqlTypeCode.String):
					//TODO: get the dest encoding and convert the string
					castedValue = new SqlString(str);
					break;
				case (SqlTypeCode.Date):
					castedValue = ToDate(str);
					break;
				case (SqlTypeCode.Time):
					castedValue = ToTime(str);
					break;
				case (SqlTypeCode.TimeStamp):
					castedValue = ToTimeStamp(str);
					break;
				case (SqlTypeCode.Blob):
				case (SqlTypeCode.Binary):
				case (SqlTypeCode.VarBinary):
				case (SqlTypeCode.LongVarBinary):
					castedValue = new SqlBinary(Encoding.Unicode.GetBytes(str));
					break;
				case (SqlTypeCode.Null):
					castedValue = SqlNull.Value;
					break;
				case (SqlTypeCode.Clob):
					// TODO: have a context where to get a new CLOB
					castedValue = new SqlString(str);
					break;
				default:
					throw new InvalidCastException();
			}

			return new DataObject(destType, castedValue);
		}

		public override int Compare(ISqlObject x, ISqlObject y) {
			if (x == null)
				throw new ArgumentNullException("x");

			if (!(x is ISqlString) ||
				!(y is ISqlString))
				throw new ArgumentException("Cannot compare objects that are not strings.");

			if (x.IsNull && y.IsNull)
				return 0;
			if (x.IsNull && !y.IsNull)
				return 1;
			if (!x.IsNull && y.IsNull)
				return -1;

			// If lexicographical ordering,
			if (Locale == null)
				return LexicographicalOrder((ISqlString)x, (ISqlString)y);

			return Collator.Compare(x.ToString(), y.ToString());
		}

		private static int LexicographicalOrder(ISqlString str1, ISqlString str2) {
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