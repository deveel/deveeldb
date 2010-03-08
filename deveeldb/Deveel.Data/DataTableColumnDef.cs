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

using Deveel.Data.Text;

namespace Deveel.Data {
	/// <summary>
	/// Used to managed all the informations about a column in a table
	/// (<see cref="DataTableDef"/>).
	/// </summary>
	public class DataTableColumnDef {
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
		private String class_constraint = "";

		/// <summary>
		/// The constraining Type object itself.
		/// </summary>
		private Type constraining_class;

		/// <summary>
		/// The actual column type input the database (as defined input
		/// <see cref="Data.DbType"/>.
		/// </summary>
		private DbType db_type;

		/// <summary>
		/// The default expression string.
		/// </summary>
		private String default_expression_string;

		/// <summary>
		/// If this is a foreign key, the table.column that this foreign key
		/// refers to.
		/// </summary>
		[Obsolete]
		private String foreign_key = "";

		/// <summary>
		/// The type of index to use on this column.
		/// </summary>
		private String index_desc = "";

		/// <summary>
		/// The locale string if this column represents a string.  If this is an
		/// empty string, the column has no locale (the string is collated
		/// lexicographically).
		/// </summary>
		private String locale_str = "";

		/// <summary>
		/// The name of the column.
		/// </summary>
		private String name;

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
		private SqlType sql_type;

		/// <summary>
		/// The locale collation decomposition if this column represents a string.
		/// </summary>
		private CollationDecomposition str_decomposition;

		/// <summary>
		/// The locale collation strength if this column represents a string.
		/// </summary>
		private CollationStrength str_strength;

		/// <summary>
		/// The TType object for this column.
		/// </summary>
		public TType type;


		///<summary>
		///</summary>
		public DataTableColumnDef() {
		}

		///<summary>
		///</summary>
		///<param name="column_def"></param>
		public DataTableColumnDef(DataTableColumnDef column_def) {
			Array.Copy(column_def.constraints_format, 0,
			           constraints_format, 0, constraints_format.Length);
			name = column_def.name;
			sql_type = column_def.sql_type;
			db_type = column_def.db_type;
			size = column_def.size;
			scale = column_def.scale;
			locale_str = column_def.locale_str;
			str_strength = column_def.str_strength;
			str_decomposition = column_def.str_decomposition;
			if (column_def.default_expression_string != null) {
				default_expression_string = column_def.default_expression_string;
				//      default_exp = new Expression(column_def.default_exp);
			}
			foreign_key = column_def.foreign_key;
			index_desc = column_def.index_desc;
			class_constraint = column_def.class_constraint;
			type = column_def.type;
		}

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
			get { return sql_type; }
			set {
				sql_type = value;
				if (value == SqlType.Bit ||
				    value == SqlType.Boolean) {
					db_type = Data.DbType.Boolean;
				} else if (value == SqlType.TinyInt ||
				           value == SqlType.SmallInt ||
				           value == SqlType.Integer ||
				           value == SqlType.BigInt ||
				           value == SqlType.Float ||
				           value == SqlType.Real ||
				           value == SqlType.Double ||
				           value == SqlType.Numeric ||
				           value == SqlType.Decimal) {
					db_type = DbType.Numeric;
				} else if (value == SqlType.Char ||
				           value == SqlType.VarChar ||
				           value == SqlType.LongVarChar) {
					db_type = DbType.String;
				} else if (value == SqlType.Date ||
				           value == SqlType.Time ||
				           value == SqlType.TimeStamp) {
					db_type = DbType.Time;
				} else if (value == SqlType.Binary ||
				           value == SqlType.VarBinary ||
				           value == SqlType.LongVarBinary) {
					db_type = DbType.Blob;
				} else if (value == SqlType.Object) {
					db_type = DbType.Object;
				} else {
					db_type = DbType.Unknown;
				}
			}
		}

		/// <summary>
		/// Returns the SQL type as a String.
		/// </summary>
		public string SQLTypeString {
			get { return SqlType.ToString().ToUpper(); }
		}

		/// <summary>
		/// Returns the type as a String.
		/// </summary>
		public string DbTypeString {
			get {
				switch (DbType) {
					case DbType.Numeric:
						return "NUMERIC";
					case DbType.String:
						return "STRING";
					case DbType.Boolean:
						return "BOOLEAN";
					case DbType.Time:
						return "TIME";
					case DbType.Blob:
						return "BLOB";
					case DbType.Object:
						return "OBJECT";
					default:
						return "UNKNOWN(" + DbType + ")";
				}
			}
		}

		/// <summary>
		/// Returns the <see cref="Type"/> of object that represents this column.
		/// </summary>
		public Type ColumnType {
			get { return TypeUtil.ToType(DbType); }
		}

		///<summary>
		///</summary>
		///<exception cref="ArgumentException"></exception>
		public DbType DbType {
			get { return db_type; }
			set {
				db_type = value;
				if (value == DbType.Numeric) {
					sql_type = SqlType.Numeric;
				} else if (value == DbType.String) {
					sql_type = SqlType.LongVarChar;
				} else if (value == DbType.Boolean) {
					sql_type = SqlType.Bit;
				} else if (value == DbType.Time) {
					sql_type = SqlType.TimeStamp;
				} else if (value == DbType.Blob) {
					sql_type = SqlType.LongVarBinary;
				} else if (value == DbType.Object) {
					sql_type = SqlType.Object;
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
			get { return locale_str; }
		}

		///<summary>
		///</summary>
		public CollationStrength Strength {
			get { return str_strength; }
		}

		///<summary>
		///</summary>
		public CollationDecomposition Decomposition {
			get { return str_decomposition; }
		}

		/// <summary>
		/// Gets or sets the name of the scheme we use to index this column.
		/// </summary>
		/// <remarks>
		/// It will be either <i>InsertSearch</i> or <b>BlindSearch</b>.
		/// </remarks>
		public string IndexScheme {
			get {
				if (index_desc.Equals("")) {
					return "InsertSearch";
				}
				return index_desc;
			}
			set { index_desc = value; }
		}

		/// <summary>
		/// Gets <b>true</b> if this type of column is able to be indexed,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsIndexableType {
			get {
				if (DbType == DbType.Blob ||
				    DbType == DbType.Object) {
					return false;
				}
				return true;
			}
		}

		///<summary>
		/// If this column represents an object, gets or sets the name of 
		/// the type the object must be derived from to be added to the
		/// column.
		///</summary>
		///<exception cref="ApplicationException"></exception>
		public string TypeConstraintString {
			get { return class_constraint; }
			set {
				class_constraint = value;
				try {
					// Denotes an array
					if (value.EndsWith("[]")) {
						String array_class =
							value.Substring(0, value.Length - 2);
						Type ac;
						// Arrays of primitive types,
						if (array_class.Equals("bool")) {
							ac = typeof (bool);
						} else if (array_class.Equals("byte")) {
							ac = typeof (byte);
						} else if (array_class.Equals("char")) {
							ac = typeof (char);
						} else if (array_class.Equals("short")) {
							ac = typeof (short);
						} else if (array_class.Equals("int")) {
							ac = typeof (int);
						} else if (array_class.Equals("long")) {
							ac = typeof (long);
						} else if (array_class.Equals("float")) {
							ac = typeof (float);
						} else if (array_class.Equals("double")) {
							ac = typeof (double);
						} else {
							// Otherwise a standard array.
							ac = Type.GetType(array_class, true, true);
						}
						// Make it into an array
						constraining_class = Array.CreateInstance(ac, 0).GetType();
					} else {
						// Not an array
						constraining_class = Type.GetType(value, true, true);
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
			get { return constraining_class; }
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
		///<param name="locale_str"></param>
		///<param name="strength"></param>
		///<param name="decomposition"></param>
		public void SetStringLocale(String locale_str,
		                            CollationStrength strength, CollationDecomposition decomposition) {
			// Sets this column to be of the given locale.  For example, the string
			// "frFR" denotes french/france.  See Deveel.Data.TStringType.cs
			// for more information.
			if (locale_str == null) {
				this.locale_str = "";
			} else {
				this.locale_str = locale_str;
				str_strength = strength;
				str_decomposition = decomposition;
			}
		}

		///<summary>
		///</summary>
		///<param name="expression"></param>
		public void SetDefaultExpression(Expression expression) {
			default_expression_string = expression.Text.ToString();
		}

		///<summary>
		/// Sets this <see cref="DataTableColumnDef"/> object up from information 
		/// input the <see cref="TType"/> object.
		///</summary>
		///<param name="type"></param>
		/// <remarks>
		/// This is useful when we need to create a <see cref="DataTableColumnDef"/>
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
		public void InitTTypeInfo() {
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
			if (default_expression_string == null) {
				return null;
			}
			Expression exp = Expression.Parse(default_expression_string);
			return exp;
		}

		///<summary>
		///</summary>
		///<returns></returns>
		public String GetDefaultExpressionString() {
			return default_expression_string;
		}

		///<summary>
		/// Returns this column as a <see cref="ColumnDescription"/> object 
		/// and gives the column description the given name.
		///</summary>
		///<param name="column_name"></param>
		///<returns></returns>
		public ColumnDescription ColumnDescriptionValue(String column_name) {
			ColumnDescription field = new ColumnDescription(column_name, DbType, Size, IsNotNull);
			field.Scale = Scale;
			field.SQLType = SqlType;

			return field;
		}

		/// <summary>
		/// Dumps information about this object to the <see cref="TextWriter"/>.
		/// </summary>
		/// <param name="output"></param>
		public void Dump(TextWriter output) {
			output.Write(Name);
			output.Write("(");
			output.Write(SQLTypeString);
			output.Write(")");
		}

		// ---------- Convenient static methods ----------

		/// <summary>
		/// Returns a TType object for a column with the given type information.
		/// </summary>
		/// <param name="sql_type"></param>
		/// <param name="size"></param>
		/// <param name="scale"></param>
		/// <param name="locale"></param>
		/// <param name="str_strength"></param>
		/// <param name="str_decomposition"></param>
		/// <param name="typeString"></param>
		/// <remarks>
		/// The type information is the sql_type, the size and the scale of the type.
		/// </remarks>
		/// <returns></returns>
		private static TType CreateTTypeFor(SqlType sql_type, int size, int scale,
		                                    String locale, CollationStrength str_strength,
		                                    CollationDecomposition str_decomposition,
		                                    String typeString) {
			switch (sql_type) {
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
					return new TNumericType(sql_type, size, scale);

				case (SqlType.Char):
				case (SqlType.VarChar):
				case (SqlType.LongVarChar):
				case (SqlType.Clob):
					return new TStringType(sql_type, size, locale,
					                       str_strength, str_decomposition);

				case (SqlType.Date):
				case (SqlType.Time):
				case (SqlType.TimeStamp):
					return new TDateType(sql_type);

				case (SqlType.Binary):
				case (SqlType.VarBinary):
				case (SqlType.LongVarBinary):
				case (SqlType.Blob):
					return new TBinaryType(sql_type, size);

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
		/// Creates a DataTableColumnDef that holds a numeric value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnDef CreateNumericColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
			column.Name = name;
			column.SqlType = SqlType.Numeric;
			column.InitTTypeInfo();
			return column;
		}

		///<summary>
		/// Creates a DataTableColumnDef that holds a boolean value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnDef CreateBooleanColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
			column.Name = name;
			column.SqlType = SqlType.Bit;
			column.InitTTypeInfo();
			return column;
		}

		///<summary>
		/// Creates a DataTableColumnDef that holds a string value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnDef CreateStringColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
			column.Name = name;
			column.SqlType = SqlType.VarChar;
			column.Size = Int32.MaxValue;
			column.InitTTypeInfo();
			return column;
		}

		///<summary>
		/// Creates a DataTableColumnDef that holds a binary value.
		///</summary>
		///<param name="name"></param>
		///<returns></returns>
		public static DataTableColumnDef CreateBinaryColumn(String name) {
			DataTableColumnDef column = new DataTableColumnDef();
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
			output.Write((int) sql_type);
			output.Write((int) db_type);
			output.Write(size);
			output.Write(scale);

			if (default_expression_string != null) {
				output.Write(true);
				output.Write(default_expression_string);
				//new String(default_exp.text().toString()));
			} else {
				output.Write(false);
			}

			output.Write(foreign_key);
			output.Write(index_desc);
			output.Write(class_constraint); // Introduced input version 2.

			// Format the 'other' string
			StringBuilder other = new StringBuilder();
			other.Append("|");
			other.Append(locale_str);
			other.Append("|");
			other.Append((int)str_strength);
			other.Append("|");
			other.Append((int)str_decomposition);
			other.Append("|");
			// And Write it
			output.Write(other.ToString());
		}

		/// <summary>
		/// Reads this column from a <see cref="BinaryReader"/>.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		internal static DataTableColumnDef Read(BinaryReader input) {
			int ver = input.ReadInt32();

			DataTableColumnDef cd = new DataTableColumnDef();
			cd.name = input.ReadString();
			int len = input.ReadInt32();
			input.Read(cd.constraints_format, 0, len);
			cd.sql_type = (SqlType) input.ReadInt32();
			cd.db_type = (DbType) input.ReadInt32();
			cd.size = input.ReadInt32();
			cd.scale = input.ReadInt32();

			bool b = input.ReadBoolean();
			if (b) {
				cd.default_expression_string = input.ReadString();
				//      cd.default_exp = Expression.Parse(input.readUTF());
			}
			cd.foreign_key = input.ReadString();
			cd.index_desc = input.ReadString();
			if (ver > 1) {
				String cc = input.ReadString();
				if (!cc.Equals("")) {
					cd.TypeConstraintString = cc;
				}
			} else {
				cd.class_constraint = "";
			}

			// Parse the 'other' string
			String other = input.ReadString();
			if (other.Length > 0) {
				if (other.StartsWith("|")) {
					// Read the string locale, collation strength and disposition
					int cur_i = 1;
					int next_break = other.IndexOf("|", cur_i);
					cd.locale_str = other.Substring(cur_i, next_break - cur_i);

					cur_i = next_break + 1;
					next_break = other.IndexOf("|", cur_i);
					cd.str_strength = (CollationStrength) Int32.Parse(other.Substring(cur_i, next_break - cur_i));

					cur_i = next_break + 1;
					next_break = other.IndexOf("|", cur_i);
					cd.str_decomposition = (CollationDecomposition) Int32.Parse(other.Substring(cur_i, next_break - cur_i));
				} else {
					throw new FormatException("Incorrectly formatted DataTableColumnDef data.");
				}
			}

			cd.InitTTypeInfo();

			return cd;
		}
	}
}