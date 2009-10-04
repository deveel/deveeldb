//  
//  TType.cs
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
using System.Collections;
using System.Text;

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// This class is the representation of a type in a database engine.
	/// </summary>
	/// <remarks>
	/// Implementations of this class might represent a <c>STRING</c> or a 
	/// <c>NUMBER</c>.
	/// <para>
	/// A <see cref="TType"/> is also a <see cref="IComparer"/> implementation,
	/// since can be used to sort and compare two objects handled by the specific
	/// implementation.
	/// </para>
	/// </remarks>
	[Serializable]
	public abstract class TType : IComparer {
		/// <summary>
		/// A type that represents an array.
		/// </summary>
		public static readonly TArrayType ArrayType = new TArrayType();

		/// <summary>
		/// A default binary (SQL BLOB) type of unlimited maximum size.
		/// </summary>
		public static readonly TBinaryType BinaryType = new TBinaryType(SQLTypes.BLOB, -1);

		/// <summary>
		/// A default boolean (SQL BIT) type.
		/// </summary>
		public static readonly TBooleanType BooleanType = new TBooleanType(SQLTypes.BIT);

		/// <summary>
		/// A default date (SQL TIMESTAMP) type.
		/// </summary>
		public static readonly TDateType DateType = new TDateType(SQLTypes.TIMESTAMP);

		/// <summary>
		/// A default NULL type.
		/// </summary>
		public static readonly TNullType NullType = new TNullType();

		/// <summary>
		/// A default numeric (SQL NUMERIC) type of unlimited size and scale.
		/// </summary>
		public static readonly TNumericType NumericType = new TNumericType(SQLTypes.NUMERIC, -1, -1);

		/// <summary>
		/// A type that represents a query plan (sub-select).
		/// </summary>
		public static readonly TQueryPlanType QueryPlanType = new TQueryPlanType();

		/// <summary>
		/// A default string (SQL VARCHAR) type of unlimited maximum size and
		/// null locale.
		/// </summary>
		public static readonly TStringType StringType = new TStringType(SQLTypes.VARCHAR, -1, null);

		/// <summary>
		/// The type as an SQL identifier from <see cref="SQLTypes"/>.
		/// </summary>
		private readonly SQLTypes sql_type;

		/// <summary>
		/// Instantiate the <see cref="TType"/> for a given <see cref="SQLTypes"/>.
		/// </summary>
		/// <param name="sql_type"></param>
		protected TType(SQLTypes sql_type) {
			this.sql_type = sql_type;
		}

		/// <summary>
		/// Returns the SQL type of this <see cref="TType"/>.
		/// </summary>
		public SQLTypes SQLType {
			get { return sql_type; }
		}

		#region IComparer Members

		/// <summary>
		/// Compares two objects that are logically comparable under this type.
		/// </summary>
		/// <param name="x">The fist instance object to compare.</param>
		/// <param name="y">The second instance object to compare.</param>
		/// <returns>
		/// Returns 0 if the values are equal, 1 if <paramref name="y"/> 
		/// is greater than <paramref name="y"/>, and -1 if <paramref name="y"/> 
		/// is less than <paramref name="x"/>.
		/// </returns>
		public abstract int Compare(Object x, Object y);

		#endregion

		/// <summary>
		/// Gets the current <see cref="TType"/> as a fully parsable declared 
		/// SQL type.
		/// </summary>
		/// <example>
		/// If this represents a string we might return "VARCHAR(30) COLLATE 'jpJP'"
		/// </example>
		/// <remarks>
		/// This method is used for debugging and display purposes only and we 
		/// would not expect to actually feed this back into an SQL parser.
		/// </remarks>
		public string ToSQLString() {
			return SQLType.ToString().ToUpper();
		}


		/// <summary>
		/// Returns <b>true</b> if the type of this object is logically comparable to the
		/// type of the given object, otherwise <b>false</b>.
		/// </summary>
		/// <param name="type">The <see cref="TType">type</see> to check.</param>
		/// <example>
		/// For example, <c>VARCHAR</c> and <c>LONGVARCHAR</c> are
		/// comparable types. <c>DOUBLE</c> and <c>FLOAT</c> are comparable types.
		/// <c>DOUBLE</c> and <c>VARCHAR</c> are not comparable types.
		/// </example>
		/// <returns>
		/// Returns <b>true</b> is the <paramref name="type"/> is comparable
		/// to the current, otherwise <b>false</b>.
		/// </returns>
		public abstract bool IsComparableType(TType type);

		/// <summary>
		/// Calculates the approximate memory usage of an object of this 
		/// type in bytes.
		/// </summary>
		/// <param name="ob">The instance to which calculate the amount of
		/// memory usage.</param>
		/// <returns>
		/// Returns an <see cref="Int32">integer</see> indicating the aproximate
		/// amount of memory needed to store the given instance.
		/// </returns>
		public abstract int CalculateApproximateMemoryUse(Object ob);

		/// <summary>
		/// Returns the <see cref="Type"/> that is used to represent this type 
		/// of object on the framework.
		/// </summary>
		/// <returns></returns>
		public abstract Type GetObjectType();


		// ----- Static methods for Encoding/Decoding TType to strings -----

		/// <summary>
		/// Returns the value of a string that is quoted. For example, 'test' becomes test.
		/// </summary>
		/// <param name="str"></param>
		/// <returns></returns>
		private static String ParseQuotedString(String str) {
			if (str.StartsWith("'") && str.EndsWith("'"))
				return str.Substring(1, str.Length - 2);
			throw new ArgumentException("String is not quoted: " + str);
		}

		/// <summary>
		/// Encodes a <see cref="TType"/> into a string which is a useful way 
		/// to serialize a <see cref="TType"/>.
		/// </summary>
		/// <param name="type">The <see cref="TType"/> instance to encode.</param>
		/// <returns>
		/// Returns a <see cref="String"/> representing the given <see cref="TType"/>
		/// in a human readable form.
		/// </returns>
		/// <exception cref="SystemException">
		/// If the given <paramref name="type"/> cannot be encoded.
		/// </exception>
		public static String Encode(TType type) {
			StringBuilder buf = new StringBuilder();
			if (type is TBooleanType) {
				buf.Append("BOOLEAN(");
				buf.Append(type.SQLType);
				buf.Append(')');
			} else if (type is TStringType) {
				TStringType str_type = (TStringType)type;
				buf.Append("STRING(");
				buf.Append(type.SQLType);
				buf.Append(',');
				buf.Append(str_type.MaximumSize);
				buf.Append(",'");
				buf.Append(str_type.LocaleString);
				buf.Append("',");
				buf.Append((int)str_type.Strength);
				buf.Append(',');
				buf.Append((int)str_type.Decomposition);
				buf.Append(')');
			} else if (type is TNumericType) {
				TNumericType num_type = (TNumericType)type;
				buf.Append("NUMERIC(");
				buf.Append(type.SQLType);
				buf.Append(',');
				buf.Append(num_type.Size);
				buf.Append(',');
				buf.Append(num_type.Scale);
				buf.Append(')');
			} else if (type is TBinaryType) {
				TBinaryType bin_type = (TBinaryType)type;
				buf.Append("BINARY(");
				buf.Append(type.SQLType);
				buf.Append(',');
				buf.Append(bin_type.MaximumSize);
				buf.Append(')');
			} else if (type is TDateType) {
				buf.Append("DATE(");
				buf.Append(type.SQLType);
				buf.Append(')');
			} else if (type is TNullType) {
				buf.Append("NULL(");
				buf.Append(type.SQLType);
				buf.Append(')');
			} else if (type is TObjectType) {
				buf.Append("OBJECT(");
				buf.Append(type.SQLType);
				buf.Append(",'");
				buf.Append(((TObjectType) type).TypeString);
				buf.Append("')");
			} else {
				throw new ArgumentException("Can not encode type: " + type);
			}
			return buf.ToString();
		}

		/// <summary>
		/// Given an array of <see cref="TType"/>, returns a <see cref="String"/> that 
		/// is the encoded form of the array and that can be later decoded back 
		/// into an array of <see cref="TType"/>.
		/// </summary>
		/// <param name="types">Array of <see cref="TType"/> to encode.</param>
		/// <returns>
		/// Returns a comma-separed string containing the string representation of
		/// the given <paramref name="types"/>.
		/// </returns>
		/// <exception cref="SystemException">
		/// If one of the specified <paramref name="types"/> cannot be encoded.
		/// </exception>
		/// <seealso cref="Encode(TType)"/>
		public static String Encode(TType[] types) {
			StringBuilder buf = new StringBuilder();
			for (int i = 0; i < types.Length; ++i) {
				buf.Append(Encode(types[i]));
				if (i < types.Length - 1) {
					buf.Append("!|");
				}
			}
			return buf.ToString();
		}

		/// <summary>
		/// Decodes a <see cref="String"/> that has been encoded with the 
		/// <see cref="Encode(TType)"/> method and returns a <see cref="TType"/> 
		/// that represented the type.
		/// </summary>
		/// <param name="encoded_str">The <see cref="String">string</see> to decode.</param>
		/// <returns>
		/// Returns a <see cref="TType"/> obtained by the decodification of the given
		/// <paramref name="encoded_str">string</paramref>.
		/// </returns>
		/// <exception cref="SystemException">
		/// If the system was unable to parse the given string or if the encoded  
		/// representation is invalid.
		/// </exception>
		public static TType DecodeString(String encoded_str) {
			int param_s = encoded_str.IndexOf('(');
			int param_e = encoded_str.LastIndexOf(')');
			String parameterss = encoded_str.Substring(param_s + 1, param_e);
			string[] param_list = parameterss.Split(',');
			SQLTypes sql_type = (SQLTypes)Int32.Parse(param_list[0]);

			if (encoded_str.StartsWith("BOOLEAN("))
				return new TBooleanType(sql_type);
			if (encoded_str.StartsWith("STRING(")) {
				int size = Int32.Parse(param_list[1]);
				String locale_str = ParseQuotedString(param_list[2]);
				if (locale_str.Length == 0) {
					locale_str = null;
				}
				Text.CollationStrength strength = (Text.CollationStrength) Int32.Parse(param_list[3]);
				Text.CollationDecomposition decomposition = (Text.CollationDecomposition) Int32.Parse(param_list[4]);
				return new TStringType(sql_type, size, locale_str, strength, decomposition);
			}
			if (encoded_str.StartsWith("NUMERIC(")) {
				int size = Int32.Parse(param_list[1]);
				int scale = Int32.Parse(param_list[2]);
				return new TNumericType(sql_type, size, scale);
			}
			if (encoded_str.StartsWith("BINARY(")) {
				int size = Int32.Parse(param_list[1]);
				return new TBinaryType(sql_type, size);
			}
			if (encoded_str.StartsWith("DATE("))
				return new TDateType(sql_type);
			if (encoded_str.StartsWith("NULL("))
				return new TNullType();
			if (encoded_str.StartsWith("OBJECT(")) {
				String class_str = ParseQuotedString(param_list[1]);
				return new TObjectType(class_str);
			}

			throw new ArgumentException("Can not parse encoded string: " + encoded_str);
		}

		/// <summary>
		/// Decodes a list (or array) of TType objects that was previously encoded
		/// with the <see cref="Encode(TType[])"/> method.
		/// </summary>
		/// <param name="encoded_str"></param>
		/// <returns></returns>
		public static TType[] DecodeTypes(String encoded_str) {
			string[] items = encoded_str.Split(new char[] {'!', '|'});

			// Handle the empty string (no args)
			if (items.Length == 1) {
				if (items[0].Equals("")) {
					return new TType[0];
				}
			}

			int sz = items.Length;
			TType[] return_types = new TType[sz];
			for (int i = 0; i < sz; ++i) {
				String str = items[i];
				return_types[i] = DecodeString(str);
			}
			return return_types;
		}


		// -----

		/// <summary>
		/// Returns a TBinaryType constrained for the given class.
		/// </summary>
		/// <param name="class_name"></param>
		/// <returns></returns>
		public static TType GetObjectType(String class_name) {
			return new TObjectType(class_name);
		}

		/// <summary>
		/// Returns a TStringType object of the given size and locale information.
		/// </summary>
		/// <param name="sql_type"></param>
		/// <param name="size"></param>
		/// <param name="locale"></param>
		/// <param name="strength"></param>
		/// <param name="decomposition"></param>
		/// <remarks>
		/// If locale is null then collation is lexicographical.
		/// </remarks>
		/// <returns></returns>
		public static TType GetStringType(SQLTypes sql_type, int size,
		                                  String locale, Text.CollationStrength strength, Text.CollationDecomposition decomposition) {
			return new TStringType(sql_type, size, locale, strength, decomposition);
		}

		/// <summary>
		/// Returns a TNumericType object of the given size and scale.
		/// </summary>
		/// <param name="sql_type"></param>
		/// <param name="size"></param>
		/// <param name="scale"></param>
		/// <returns></returns>
		public static TType GetNumericType(SQLTypes sql_type, int size, int scale) {
			return new TNumericType(sql_type, size, scale);
		}

		/// <summary>
		/// Returns a TBooleanType object.
		/// </summary>
		/// <param name="sql_type"></param>
		/// <returns></returns>
		public static TType GetBooleanType(SQLTypes sql_type) {
			return new TBooleanType(sql_type);
		}

		/// <summary>
		/// Returns a TDateType object.
		/// </summary>
		/// <param name="sql_type"></param>
		/// <returns></returns>
		public static TType GetDateType(SQLTypes sql_type) {
			return new TDateType(sql_type);
		}

		/// <summary>
		/// Returns a TBinaryType object.
		/// </summary>
		/// <param name="sql_type"></param>
		/// <param name="size"></param>
		/// <returns></returns>
		public static TType GetBinaryType(SQLTypes sql_type, int size) {
			return new TBinaryType(sql_type, size);
		}

		// -----

		/// <summary>
		/// Casts the given <see cref="System.Object"/> to the given type.
		/// </summary>
		/// <param name="ob"></param>
		/// <param name="type"></param>
		/// <example>
		/// For example, given a <see cref="BigNumber"/> object and 
		/// <see cref="TStringType"/>, this would return the number as a string.
		/// </example>
		/// <returns></returns>
		public static Object CastObjectToTType(Object ob, TType type) {
			// Handle the null case
			if (ob == null) {
				return null;
			}

			int size = -1;
			int scale = -1;
			SQLTypes sql_type = type.SQLType;

			if (type is TStringType) {
				size = ((TStringType) type).MaximumSize;
			} else if (type is TNumericType) {
				TNumericType num_type = (TNumericType)type;
				size = num_type.Size;
				scale = num_type.Scale;
			} else if (type is TBinaryType) {
				size = ((TBinaryType) type).MaximumSize;
			}

			ob = CastHelper.CastToSQLType(ob, type.SQLType, size, scale);

			return ob;
		}


		/// <summary>
		/// Given a <see cref="Type"/>, this will return a default 
		/// <see cref="TType"/> object that can encapsulate objects 
		/// of this type.
		/// </summary>
		/// <param name="c"></param>
		/// <remarks>
		/// For example, given <see cref="string"/>, this will return 
		/// a <see cref="TStringType"/> with no locale and maximum size.
		/// <para>
		/// Note that using this method is generally not recommended unless you
		/// really can't determine more type information than from the object
		/// itself.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public static TType FromType(Type c) {
			if (c == typeof (String))
				return StringType;
			if (c == typeof (BigNumber))
				return NumericType;
			if (c == typeof (DateTime))
				return DateType;
			if (c == typeof (Boolean))
				return BooleanType;
			if (c == typeof (ByteLongObject))
				return BinaryType;

			throw new ArgumentException("Don't know how to convert " + c + " to a TType.");
		}


		/// <summary>
		/// Assuming that the two types are numeric types, this will return the
		/// <i>widest</i> of the two types.
		/// </summary>
		/// <param name="t1"></param>
		/// <param name="t2"></param>
		/// <example>
		/// For example, an <c>INTEGER</c> is a wider type than a
		/// <c>SHORT</c>, and a <c>FLOAT</c> is wider than an <c>INTEGER</c>.
		/// </example>
		/// <returns></returns>
		public static TType GetWidestType(TType t1, TType t2) {
			SQLTypes t1SQLType = t1.SQLType;
			SQLTypes t2SQLType = t2.SQLType;
			if (t1SQLType == SQLTypes.DECIMAL) {
				return t1;
			}
			if (t2SQLType == SQLTypes.DECIMAL) {
				return t2;
			}
			if (t1SQLType == SQLTypes.NUMERIC) {
				return t1;
			}
			if (t2SQLType == SQLTypes.NUMERIC) {
				return t2;
			}

			if (t1SQLType == SQLTypes.BIT) {
				return t2; // It can't be any smaller than a BIT
			}
			if (t2SQLType == SQLTypes.BIT) {
				return t1;
			}

			int t1IntSize = GetIntSize(t1SQLType);
			int t2IntSize = GetIntSize(t2SQLType);
			if (t1IntSize > 0 && t2IntSize > 0) {
				// Both are int types, use the largest size
				return (t1IntSize > t2IntSize) ? t1 : t2;
			}

			int t1FloatSize = GetFloatSize(t1SQLType);
			int t2FloatSize = GetFloatSize(t2SQLType);
			if (t1FloatSize > 0 && t2FloatSize > 0) {
				// Both are floating types, use the largest size
				return (t1FloatSize > t2FloatSize) ? t1 : t2;
			}

			if (t1FloatSize > t2IntSize) {
				return t1;
			}
			if (t2FloatSize > t1IntSize) {
				return t2;
			}
			if (t1IntSize >= t2FloatSize || t2IntSize >= t1FloatSize) {
				// Must be a long (8 bytes) and a real (4 bytes), widen to a double
				return new TNumericType(SQLTypes.DOUBLE, 8, -1);
			}
			// NOTREACHED - can't get here, the last three if statements cover
			// all possibilities.
			throw new ApplicationException("Widest type error.");
		}


		/// <summary>
		/// Get the number of bytes used by an integer type.
		/// </summary>
		/// <param name="sqlType">The SQL type.</param>
		/// <returns>
		/// Returns the number of bytes required for data of 
		/// that type, or 0 if not an int type.
		/// </returns>
		private static int GetIntSize(SQLTypes sqlType) {
			switch (sqlType) {
				case SQLTypes.TINYINT:
					return 1;
				case SQLTypes.SMALLINT:
					return 2;
				case SQLTypes.INTEGER:
					return 4;
				case SQLTypes.BIGINT:
					return 8;
				default:
					return 0;
			}
		}


		/// <summary>
		/// Get the number of bytes used by a floating type.
		/// </summary>
		/// <param name="sqlType">The SQL type.</param>
		/// <returns>
		/// Returns the number of bytes required for data of 
		/// that type, or 0 if not an int type.
		/// </returns>
		private static int GetFloatSize(SQLTypes sqlType) {
			switch (sqlType) {
				default:
					return 0;
				case SQLTypes.REAL:
					return 4;
				case SQLTypes.FLOAT:
				case SQLTypes.DOUBLE:
					return 8;
			}
		}
	}
}