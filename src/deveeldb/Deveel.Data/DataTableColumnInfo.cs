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
using System.IO;
using System.Text;

using Deveel.Data.Protocol;
using Deveel.Data.Text;

namespace Deveel.Data {
	/// <summary>
	/// Used to managed all the informations about a column in a table
	/// (<see cref="DataTableInfo"/>).
	/// </summary>
	public sealed class DataTableColumnInfo : ICloneable {
		/// <summary>
		/// An array of bytes containing the column constraints 
		/// format information.
		/// </summary>
		private readonly byte[] constraints_format = new byte[16];

		/// <summary>
		/// If this is an object column, this is a constraint that the object
		/// must be derived from to be added to this column.  If not specified,
		/// it defaults to <see cref="object"/>.
		/// </summary>
		private string typeConstraint = "";

		/// <summary>
		/// The constraining Type object itself.
		/// </summary>
		private Type constrainingType;

		/// <summary>
		/// The actual column type input the database (as defined input
		/// <see cref="Data.DbType"/>.
		/// </summary>
		private DbType dbType;

		/// <summary>
		/// The default expression string.
		/// </summary>
		private string defaultExpressionString;

		/// <summary>
		/// If this is a foreign key, the table.column that this foreign key
		/// refers to.
		/// </summary>
		[Obsolete]
		private string foreignKey = "";

		/// <summary>
		/// The type of index to use on this column.
		/// </summary>
		private string indexDesc = "";

		/// <summary>
		/// The locale string if this column represents a string.  If this is an
		/// empty string, the column has no locale (the string is collated
		/// lexicographically).
		/// </summary>
		private string localeString = "";

		/// <summary>
		/// The name of the column.
		/// </summary>
		private string name;

		/// <summary>
		/// The scale of the data.
		/// </summary>
		private int scale;

		/// <summary>
		/// The size of the data.
		/// </summary>
		private int size;

		/// <summary>
		/// The SQL type of the column.
		/// </summary>
		private SqlType sqlType;

		/// <summary>
		/// The locale collation decomposition if this column represents a string.
		/// </summary>
		private CollationDecomposition stringDecomposition;

		/// <summary>
		/// The locale collation strength if this column represents a string.
		/// </summary>
		private CollationStrength stringStrength;

		/// <summary>
		/// The TType object for this column.
		/// </summary>
		public TType type;


		// ---------- Set methods ----------

		///<summary>
		///</summary>
		public string Name {
			set { name = value; }
			get { return name; }
		}


		// ---------- Get methods ----------

		///<summary>
		///</summary>
		public bool IsNotNull {
			get { return constraints_format[0] != 0; }
			set { constraints_format[0] = (byte) (value ? 1 : 0); }
		}

		public SqlType SqlType {
			get { return sqlType; }
			set {
				sqlType = value;
				if (value == SqlType.Bit ||
				    value == SqlType.Boolean) {
					dbType = DbType.Boolean;
				} else if (value == SqlType.TinyInt ||
				           value == SqlType.SmallInt ||
				           value == SqlType.Integer ||
				           value == SqlType.BigInt ||
				           value == SqlType.Float ||
				           value == SqlType.Real ||
				           value == SqlType.Double ||
				           value == SqlType.Numeric ||
				           value == SqlType.Decimal) {
					dbType = DbType.Numeric;
				} else if (value == SqlType.Char ||
				           value == SqlType.VarChar ||
				           value == SqlType.LongVarChar) {
					dbType = DbType.String;
				} else if (value == SqlType.Date ||
				           value == SqlType.Time ||
				           value == SqlType.TimeStamp) {
					dbType = DbType.Time;
				} else if (value == SqlType.Binary ||
				           value == SqlType.VarBinary ||
				           value == SqlType.LongVarBinary) {
					dbType = DbType.Blob;
				} else if (value == SqlType.Object) {
					dbType = DbType.Object;
				} else {
					dbType = DbType.Unknown;
				}
			}
		}

		/// <summary>
		/// Returns the SQL type as a String.
		/// </summary>
		public string SQLTypeString {
			get { return SqlType.ToString().ToUpper(); }
		}
		///<summary>
		///</summary>
		///<exception cref="ArgumentException"></exception>
		public DbType DbType {
			get { return dbType; }
			set {
				dbType = value;
				if (value == DbType.Numeric) {
					sqlType = SqlType.Numeric;
				} else if (value == DbType.String) {
					sqlType = SqlType.LongVarChar;
				} else if (value == DbType.Boolean) {
					sqlType = SqlType.Bit;
				} else if (value == DbType.Time) {
					sqlType = SqlType.TimeStamp;
				} else if (value == DbType.Blob) {
					sqlType = SqlType.LongVarBinary;
				} else if (value == DbType.Object) {
					sqlType = SqlType.Object;
				} else {
					throw new ArgumentException("Unrecognised internal type.");
				}
			}
		}

		///<summary>
		///</summary>
		public int Size {
			get { return size; }
			set { size = value; }
		}

		///<summary>
		///</summary>
		public int Scale {
			get { return scale; }
			set { scale = value; }
		}

		///<summary>
		///</summary>
		public string LocaleString {
			get { return localeString; }
		}

		///<summary>
		///</summary>
		public CollationStrength Strength {
			get { return stringStrength; }
		}

		///<summary>
		///</summary>
		public CollationDecomposition Decomposition {
			get { return stringDecomposition; }
		}

		/// <summary>
		/// Gets or sets the name of the scheme we use to index this column.
		/// </summary>
		/// <remarks>
		/// It will be either <i>InsertSearch</i> or <b>BlindSearch</b>.
		/// </remarks>
		public string IndexScheme {
			get { return String.IsNullOrEmpty(indexDesc) ? "InsertSearch" : indexDesc; }
			set { indexDesc = value; }
		}

		/// <summary>
		/// Gets <b>true</b> if this type of column is able to be indexed,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsIndexableType {
			get { return DbType != DbType.Blob && DbType != DbType.Object; }
		}

		///<summary>
		/// If this column represents an object, gets or sets the name of 
		/// the type the object must be derived from to be added to the
		/// column.
		///</summary>
		///<exception cref="ApplicationException"></exception>
		public string TypeConstraintString {
			get { return typeConstraint; }
			set {
				typeConstraint = value;
				try {
					// Denotes an array
					if (value.EndsWith("[]")) {
						string arrayTypeName = value.Substring(0, value.Length - 2);
						Type arrayType;
						// Arrays of primitive types,
						if (arrayTypeName.Equals("bool")) {
							arrayType = typeof (bool);
						} else if (arrayTypeName.Equals("byte")) {
							arrayType = typeof (byte);
						} else if (arrayTypeName.Equals("char")) {
							arrayType = typeof (char);
						} else if (arrayTypeName.Equals("short")) {
							arrayType = typeof (short);
						} else if (arrayTypeName.Equals("int")) {
							arrayType = typeof (int);
						} else if (arrayTypeName.Equals("long")) {
							arrayType = typeof (long);
						} else if (arrayTypeName.Equals("float")) {
							arrayType = typeof (float);
						} else if (arrayTypeName.Equals("double")) {
							arrayType = typeof (double);
						} else {
							// Otherwise a standard array.
							arrayType = Type.GetType(arrayTypeName, true, true);
						}
						// Make it into an array
						constrainingType = Array.CreateInstance(arrayType, 0).GetType();
					} else {
						// Not an array
						constrainingType = Type.GetType(value, true, true);
					}
				} catch (TypeLoadException) {
					throw new ApplicationException("Unable to resolve class: " + value);
				}
			}
		}

		/// <summary>
		/// If this column represents a <see cref="System.Object"/>, this returns the
		/// <see cref="System.Type"/> the objects stored in the column must be derived from.
		/// </summary>
		public Type TypeConstraint {
			get { return constrainingType; }
		}

		/// <summary>
		/// Returns the TType for this column.
		/// </summary>
		public TType TType {
			get {
				if (type == null)
					throw new ApplicationException("'type' variable was not set.");
				return type;
			}
		}

		///<summary>
		///</summary>
		///<param name="locale"></param>
		///<param name="strength"></param>
		///<param name="decomposition"></param>
		public void SetStringLocale(string locale, CollationStrength strength, CollationDecomposition decomposition) {
			// Sets this column to be of the given locale.  For example, the string
			// "frFR" denotes french/france.  See Deveel.Data.TStringType.cs
			// for more information.
			if (locale == null) {
				localeString = "";
			} else {
				localeString = locale;
				stringStrength = strength;
				stringDecomposition = decomposition;
			}
		}

		///<summary>
		///</summary>
		///<param name="expression"></param>
		public void SetDefaultExpression(Expression expression) {
			defaultExpressionString = expression.Text.ToString();
		}

		///<summary>
		/// Sets this <see cref="DataTableColumnInfo"/> object up from information 
		/// input the <see cref="TType"/> object.
		///</summary>
		///<param name="type"></param>
		/// <remarks>
		/// This is useful when we need to create a <see cref="DataTableColumnInfo"/>
		/// object to store information based on nothing more than a <see cref="TType"/> 
		/// object.  This comes input useful for purely functional tables.
		/// </remarks>
		///<exception cref="ApplicationException"></exception>
		public void SetFromTType(TType type) {
			SqlType = type.SQLType;
			if (type is TStringType) {
				TStringType str_type = (TStringType)type;
				Size = str_type.MaximumSize;
				SetStringLocale(str_type.LocaleString,
				                str_type.Strength, str_type.Decomposition);
			} else if (type is TNumericType) {
				TNumericType num_type = (TNumericType)type;
				Size = num_type.Size;
				Scale = num_type.Scale;
			} else if (type is TBooleanType) {
				// Nothing necessary for booleans
				//      TBooleanType bool_type = (TBooleanType) type;
			} else if (type is TDateType) {
				// Nothing necessary for dates
				//      TDateType date_type = (TDateType) type;
			} else if (type is TNullType) {
				// Nothing necessary for nulls
			} else if (type is TBinaryType) {
				TBinaryType binary_type = (TBinaryType)type;
				Size = binary_type.MaximumSize;
			} else if (type is TObjectType) {
				TObjectType objectType = (TObjectType)type;
				TypeConstraintString = objectType.TypeString;
			} else {
				throw new ApplicationException("Don't know how to handle this type: " +
				                               type.GetType());
			}
			this.type = type;
		}

		/// <summary>
		/// Initializes the TType information for a column.
		/// </summary>
		/// <remarks>
		/// This should be called at the last part of a DataColumn setup.
		/// </remarks>
		internal void InitTTypeInfo() {
			if (type == null) {
				type = CreateTTypeFor(SqlType, Size, Scale,
				                      LocaleString, Strength, Decomposition,
				                      TypeConstraintString);
			}
		}

		///<summary>
		///</summary>
		///<param name="system"></param>
		///<returns></returns>
		public Expression GetDefaultExpression(TransactionSystem system) {
			if (defaultExpressionString == null)
				return null;

			Expression exp = Expression.Parse(defaultExpressionString);
			return exp;
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public String GetDefaultExpressionString() {
			return defaultExpressionString;
		}

		///<summary>
		/// Returns this column as a <see cref="ColumnDescription"/> object 
		/// and gives the column description the given name.
		///</summary>
		///<param name="column_name"></param>
		///<returns></returns>
		internal ColumnDescription ColumnDescriptionValue(String column_name) {
			ColumnDescription field = new ColumnDescription(column_name, DbType, Size, IsNotNull);
			field.Scale = Scale;
			field.SQLType = SqlType;

			return field;
		}

		// ---------- Convenient static methods ----------

		/// <summary>
		/// Returns a TType object for a column with the given type information.
		/// </summary>
		/// <param name="sqlType"></param>
		/// <param name="size"></param>
		/// <param name="scale"></param>
		/// <param name="locale"></param>
		/// <param name="strength"></param>
		/// <param name="decomposition"></param>
		/// <param name="typeString"></param>
		/// <remarks>
		/// The type information is the sqlType, the size and the scale of the type.
		/// </remarks>
		/// <returns></returns>
		private static TType CreateTTypeFor(SqlType sqlType, int size, int scale, string locale, CollationStrength strength, CollationDecomposition decomposition, string typeString) {
			switch (sqlType) {
				case (SqlType.Bit):
				case (SqlType.Boolean):
					return TType.BooleanType;

				case (SqlType.TinyInt):
				case (SqlType.SmallInt):
				case (SqlType.Integer):
				case (SqlType.BigInt):
				case (SqlType.Float):
				case (SqlType.Real):
				case (SqlType.Double):
				case (SqlType.Numeric):
				case (SqlType.Decimal):
				case (SqlType.Identity):
					return new TNumericType(sqlType, size, scale);

				case (SqlType.Char):
				case (SqlType.VarChar):
				case (SqlType.LongVarChar):
				case (SqlType.Clob):
					return new TStringType(sqlType, size, locale, strength, decomposition);

				case (SqlType.Date):
				case (SqlType.Time):
				case (SqlType.TimeStamp):
					return new TDateType(sqlType);

				case (SqlType.Binary):
				case (SqlType.VarBinary):
				case (SqlType.LongVarBinary):
				case (SqlType.Blob):
					return new TBinaryType(sqlType, size);

				case (SqlType.Object):
					return new TObjectType(typeString);

				case (SqlType.Array):
					return TType.ArrayType;

				case (SqlType.Null):
					return TType.NullType;

				default:
					throw new ApplicationException("SQL type not recognized.");
			}
		}

		///<summary>
		/// Creates a DataTableColumnInfo that holds a numeric value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnInfo CreateNumericColumn(String name) {
			DataTableColumnInfo column = new DataTableColumnInfo();
			column.Name = name;
			column.SqlType = SqlType.Numeric;
			column.InitTTypeInfo();
			return column;
		}

		///<summary>
		/// Creates a DataTableColumnInfo that holds a boolean value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnInfo CreateBooleanColumn(String name) {
			DataTableColumnInfo column = new DataTableColumnInfo();
			column.Name = name;
			column.SqlType = SqlType.Bit;
			column.InitTTypeInfo();
			return column;
		}

		///<summary>
		/// Creates a DataTableColumnInfo that holds a string value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnInfo CreateStringColumn(String name) {
			DataTableColumnInfo column = new DataTableColumnInfo();
			column.Name = name;
			column.SqlType = SqlType.VarChar;
			column.Size = Int32.MaxValue;
			column.InitTTypeInfo();
			return column;
		}

		///<summary>
		/// Creates a DataTableColumnInfo that holds a binary value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnInfo CreateBinaryColumn(String name) {
			DataTableColumnInfo column = new DataTableColumnInfo();
			column.Name = name;
			column.SqlType = SqlType.LongVarBinary;
			column.Size = Int32.MaxValue;
			column.IndexScheme = "BlindSearch";
			column.InitTTypeInfo();
			return column;
		}


		// ---------- IO Methods ----------

		/// <summary>
		/// Writes this column information output to a <see cref="BinaryWriter"/>.
		/// </summary>
		/// <param name="output"></param>
		internal void Write(BinaryWriter output) {
			output.Write(2); // The version

			output.Write(name);
			output.Write(constraints_format.Length);
			output.Write(constraints_format);
			output.Write((int) sqlType);
			output.Write((int) dbType);
			output.Write(size);
			output.Write(scale);

			if (defaultExpressionString != null) {
				output.Write(true);
				output.Write(defaultExpressionString);
				//new String(default_exp.text().toString()));
			} else {
				output.Write(false);
			}

			output.Write(foreignKey);
			output.Write(indexDesc);
			output.Write(typeConstraint); // Introduced input version 2.

			// Format the 'other' string
			StringBuilder other = new StringBuilder();
			other.Append("|");
			other.Append(localeString);
			other.Append("|");
			other.Append((int)stringStrength);
			other.Append("|");
			other.Append((int)stringDecomposition);
			other.Append("|");
			// And Write it
			output.Write(other.ToString());
		}

		/// <summary>
		/// Reads this column from a <see cref="BinaryReader"/>.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		internal static DataTableColumnInfo Read(BinaryReader input) {
			int ver = input.ReadInt32();

			DataTableColumnInfo cd = new DataTableColumnInfo();
			cd.name = input.ReadString();
			int len = input.ReadInt32();
			input.Read(cd.constraints_format, 0, len);
			cd.sqlType = (SqlType) input.ReadInt32();
			cd.dbType = (DbType) input.ReadInt32();
			cd.size = input.ReadInt32();
			cd.scale = input.ReadInt32();

			bool b = input.ReadBoolean();
			if (b) {
				cd.defaultExpressionString = input.ReadString();
				//      cd.default_exp = Expression.Parse(input.readUTF());
			}
			cd.foreignKey = input.ReadString();
			cd.indexDesc = input.ReadString();
			if (ver > 1) {
				String cc = input.ReadString();
				if (!cc.Equals("")) {
					cd.TypeConstraintString = cc;
				}
			} else {
				cd.typeConstraint = "";
			}

			// Parse the 'other' string
			string other = input.ReadString();
			if (other.Length > 0) {
				if (other.StartsWith("|")) {
					// Read the string locale, collation strength and disposition
					int cur_i = 1;
					int next_break = other.IndexOf("|", cur_i);
					cd.localeString = other.Substring(cur_i, next_break - cur_i);

					cur_i = next_break + 1;
					next_break = other.IndexOf("|", cur_i);
					cd.stringStrength = (CollationStrength) Int32.Parse(other.Substring(cur_i, next_break - cur_i));

					cur_i = next_break + 1;
					next_break = other.IndexOf("|", cur_i);
					cd.stringDecomposition = (CollationDecomposition) Int32.Parse(other.Substring(cur_i, next_break - cur_i));
				} else {
					throw new FormatException("Incorrectly formatted DataTableColumnInfo data.");
				}
			}

			cd.InitTTypeInfo();

			return cd;
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DataTableColumnInfo Clone() {
			DataTableColumnInfo columnInfo = new DataTableColumnInfo();
			Array.Copy(constraints_format, 0, columnInfo.constraints_format, 0, constraints_format.Length);
			columnInfo.name = name;
			columnInfo.sqlType = sqlType;
			columnInfo.dbType = dbType;
			columnInfo.size = size;
			columnInfo.scale = scale;
			columnInfo.localeString = localeString;
			columnInfo.stringStrength = stringStrength;
			columnInfo.stringDecomposition = stringDecomposition;
			if (defaultExpressionString != null) {
				columnInfo.defaultExpressionString = defaultExpressionString;
			}
			columnInfo.foreignKey = foreignKey;
			columnInfo.indexDesc = indexDesc;
			columnInfo.typeConstraint = typeConstraint;
			columnInfo.type = type;
			return columnInfo;
		}
	}
}