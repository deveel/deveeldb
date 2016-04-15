// 
//  Copyright 2010-2016 Deveel
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
//


using System;
using System.Globalization;
using System.IO;
using System.Runtime.Serialization;
using System.Text;

using Deveel.Data.Serialization;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Store;
using Deveel.Data.Text;

namespace Deveel.Data.Sql.Types {
	[Serializable]
	public sealed class StringType : SqlType, ISizeableType {
		private CompareInfo collator;

		public const int DefaultMaxSize = Int16.MaxValue;

		public StringType(SqlTypeCode typeCode, int maxSize, Encoding encoding, CultureInfo locale) 
			: base("STRING", typeCode) {
			if (encoding == null)
				throw new ArgumentNullException("encoding");

			AssertIsString(typeCode);
			MaxSize = maxSize;
			Locale = locale;
			Encoding = encoding;
		}

		private StringType(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			MaxSize = info.GetInt32("MaxSize");

			var locale = info.GetString("Locale");
			if (!String.IsNullOrEmpty(locale))
				Locale = new CultureInfo(locale);

			var encoding = info.GetString("Encoding");
			if (!String.IsNullOrEmpty(encoding))
				Encoding = Encoding.GetEncoding(encoding);
		}

		private static void AssertIsString(SqlTypeCode sqlType) {
			if (!IsStringType(sqlType))
				throw new ArgumentException(String.Format("The type {0} is not a valid STRING type.", sqlType), "sqlType");
		}

		/// <summary>
		/// Gets the maximum number of characters that strings
		/// handled by this type can handle.
		/// </summary>
		public int MaxSize { get; private set; }

		int ISizeableType.Size {
			get { return MaxSize; }
		}

		public override bool IsStorable {
			get { return true; }
		}

		/// <summary>
		/// Gets the locale used to compare string values.
		/// </summary>
		/// <remarks>
		/// When this value is not specified, the schema or database locale
		/// is used to compare string values.
		/// </remarks>
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

		public Encoding Encoding { get; private set; }

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("MaxSize", MaxSize);

			if (Locale != null) {
				info.AddValue("Locale", Locale.Name);
			} else {
				info.AddValue("Locale", null);
			}

			if (Encoding != null) {
				info.AddValue("Encoding", Encoding.WebName);
			} else {
				info.AddValue("Encoding", null);
			}
		}

		public override bool IsCacheable(ISqlObject value) {
			return value is SqlString || value is SqlNull || value == null;
		}

		/// <inheritdoc/>
		public override string ToString() {
			var sb = new StringBuilder(TypeCode.ToString().ToUpperInvariant());
			if (MaxSize >= 0)
				sb.AppendFormat("({0})", MaxSize);

			return sb.ToString();
		}

		public override Type GetObjectType() {
			if (TypeCode == SqlTypeCode.Char ||
			    TypeCode == SqlTypeCode.VarChar ||
			    TypeCode == SqlTypeCode.String)
				return typeof (SqlString);

			if (TypeCode == SqlTypeCode.LongVarChar ||
			    TypeCode == SqlTypeCode.Clob)
				return typeof (SqlLongString);

			return base.GetObjectType();
		}

		public override Type GetRuntimeType() {
			if (TypeCode == SqlTypeCode.Char ||
			    TypeCode == SqlTypeCode.VarChar ||
			    TypeCode == SqlTypeCode.String)
				return typeof (string);

			if (TypeCode == SqlTypeCode.LongVarChar ||
			    TypeCode == SqlTypeCode.Clob)
				return typeof (Stream);

			return base.GetRuntimeType();
		}

		/// <inheritdoc/>
		public override bool Equals(SqlType other) {
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

		/// <inheritdoc/>
		public override int GetHashCode() {
			return TypeCode.GetHashCode() ^ MaxSize.GetHashCode();
		}

		/// <inheritdoc/>
		public override bool IsComparable(SqlType type) {
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

		public SqlBoolean IsLike(ISqlString value, ISqlString pattern) {
			if (value == null && IsNull)
				return true;
			if (!IsNull && value == null)
				return false;
			if (value == null)
				return false;

			var s1 = value.ToString();
			var s2 = pattern.ToString();
			return PatternSearch.FullPatternMatch(s1, s2, '\\');
		}

		public SqlBoolean IsNotLike(ISqlString value, ISqlString pattern) {
			var likeResult = IsLike(value, pattern);
			if (likeResult.IsNull)
				return likeResult;

			return likeResult.Not();
		}

		public override object ConvertTo(ISqlObject obj, Type destType) {
			if (!(obj is ISqlString))
				throw new ArgumentException();

			var s = (ISqlString) obj;
			if (s.IsNull)
				return null;

			if (destType == typeof (string))
				return s.ToString();

			throw new InvalidCastException();
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


		private SqlDateTime ToDate(string str) {
			SqlDateTime result;
			if (!SqlDateTime.TryParseDate(str, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.Date, DateType.DateFormatSql));

			return result;
		}

		private SqlDateTime ToTime(String str) {
			SqlDateTime result;
			if (!SqlDateTime.TryParseTime(str, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.Time, DateType.TimeFormatSql));

			return result;

		}

		private SqlDateTime ToTimeStamp(String str) {
			SqlDateTime result;
			if (!SqlDateTime.TryParseTimeStamp(str, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.TimeStamp, DateType.TsFormatSql));

			return result;
		}

		private SqlDateTime ToDateTime(string str) {
			SqlDateTime result;
			if (!SqlDateTime.TryParse(str, out result))
				throw new InvalidCastException(DateErrorMessage(str, SqlTypeCode.TimeStamp, DateType.TsFormatSql));

			return result;
		}

		private string DateErrorMessage(string str, SqlTypeCode sqlType, string[] formats) {
			return String.Format("The input string {0} of type {1} is not compatible with any of the formats for SQL Type {2} ( {3} )",
				str,
				TypeCode.ToString().ToUpperInvariant(),
				sqlType.ToString().ToUpperInvariant(),
				String.Join(", ", formats));
		}

		private SqlBoolean ToBoolean(string s) {
			SqlBoolean value;
			if (SqlBoolean.TryParse(s, out value))
				return value;

			if (String.Equals(s, "1"))
				return SqlBoolean.True;
			if (String.Equals(s, "2"))
				return SqlBoolean.False;

			throw new InvalidCastException(String.Format("Could not convert string '{0}' of type '{1}' to BOOLEAN", s, TypeCode.ToString().ToUpperInvariant()));
		}

		/// <inheritdoc/>
		public override bool CanCastTo(SqlType destType) {
			return destType.TypeCode != SqlTypeCode.Array &&
			       destType.TypeCode != SqlTypeCode.ColumnType &&
			       destType.TypeCode != SqlTypeCode.RowType &&
			       destType.TypeCode != SqlTypeCode.Object &&
				   destType.TypeCode != SqlTypeCode.Clob &&
				   destType.TypeCode != SqlTypeCode.Blob;
		}

		/// <inheritdoc/>
		public override ISqlObject CastTo(ISqlObject value, SqlType destType) {
			string str = value.ToString();
			var sqlType = destType.TypeCode;
			ISqlObject castedValue;

			if (value.IsNull)
				return SqlNull.Value;

			switch (sqlType) {
				case (SqlTypeCode.Bit):
				case (SqlTypeCode.Boolean):
					castedValue = ToBoolean(str);
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
				case (SqlTypeCode.DateTime):
					castedValue = ToDateTime(str);
					break;
				case (SqlTypeCode.Binary):
				case (SqlTypeCode.VarBinary):
				case (SqlTypeCode.LongVarBinary):
					castedValue = new SqlBinary(Encoding.Unicode.GetBytes(str));
					break;
				case (SqlTypeCode.Null):
					castedValue = SqlNull.Value;
					break;
				default:
					throw new InvalidCastException();
			}

			return castedValue;
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

			// TODO: pick one of the two encodings?

			// The minimum size
			long size = System.Math.Min(str1Size, str2Size);
			TextReader r1 = str1.GetInput(str1.Encoding);
			TextReader r2 = str2.GetInput(str2.Encoding);
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
					r1.Dispose();
					r2.Dispose();
				}
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream);

			if (obj.IsNull) {
				if (TypeCode == SqlTypeCode.Clob ||
				    TypeCode == SqlTypeCode.LongVarChar) {
					writer.Write((byte)3);
				} else {
					writer.Write((byte)1);
				}

				return;
			}

			var sqlString = (ISqlString)obj;

			if (obj is SqlString) {
				var bytes = ((SqlString) sqlString).ToByteArray(Encoding);
				writer.Write((byte) 2);
				writer.Write(bytes.Length);
				writer.Write(bytes);
			} else if (obj is SqlLongString) {
				var longString = (SqlLongString) sqlString;
				writer.Write((byte) 4);
				writer.Write(longString.ObjectId.StoreId);
				writer.Write(longString.ObjectId.Id);
			} else {
				throw new FormatException(String.Format("The object of type '{0}' is not handled by {1}", obj.GetType(), ToString()));
			}
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream);
			var type = reader.ReadByte();

			if (type == 1)
				return SqlString.Null;
			if (type == 3)
				return SqlLongString.Null;

			if (type == 2) {
				var length = reader.ReadInt32();
				var bytes = reader.ReadBytes(length);

				var chars = Encoding.GetChars(bytes);

				return new SqlString(chars);
			}

			if (type == 4) {
				var storeId = reader.ReadInt32();
				var objId = reader.ReadInt64();
				var refObjId = new ObjectId(storeId, objId);

				// TODO: find the store and get the object
				throw new NotImplementedException();
			}

			throw new FormatException("Invalid type code in deserialization.");
		}

		internal override int ColumnSizeOf(ISqlObject obj) {
			if (obj.IsNull)
				return 1;
			if (obj is SqlString) {
				var s = (SqlString) obj;
				var length = s.GetByteCount(Encoding);

				// Type + Byte Length + Bytes
				return 1 + 4 + length;
			} 
			if (obj is SqlLongString) {
				// Type + Store ID + Object ID
				return 1 + 4 + 8;
			}

			throw new ArgumentException();
		}

		internal static bool IsStringType(SqlTypeCode typeCode) {
			return typeCode == SqlTypeCode.String ||
			typeCode == SqlTypeCode.VarChar ||
			typeCode == SqlTypeCode.Char ||
			typeCode == SqlTypeCode.LongVarChar ||
			typeCode == SqlTypeCode.Clob;
		}
	}
}