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

using Deveel.Data.Protocol;

namespace Deveel.Data {
	/// <summary>
	/// Used to managed all the informations about a column in a table
	/// (<see cref="DataTableInfo"/>).
	/// </summary>
	[Serializable]
	public sealed class DataTableColumnInfo : ICloneable {
		private DataTableInfo tableInfo;

		/// <summary>
		/// A flag indicating if the column must allow only not-null values.
		/// </summary>
		private bool notNull;

		/// <summary>
		/// If this is an object column, this is a constraint that the object
		/// must be derived from to be added to this column.  If not specified,
		/// it defaults to <see cref="object"/>.
		/// </summary>
		private String baseTypeConstraint = "";

		/// <summary>
		/// The constraining Type object itself.
		/// </summary>
		private Type baseType;

		/// <summary>
		/// The default expression string.
		/// </summary>
		private string defaultExpressionString;

		/// <summary>
		/// The type of index to use on this column.
		/// </summary>
		private string indexType = "";

		/// <summary>
		/// The name of the column.
		/// </summary>
		private string name;

		/// <summary>
		/// The TType object for this column.
		/// </summary>
		private readonly TType type;

		internal DataTableColumnInfo(DataTableInfo tableInfo, string name, TType type) {
			if (name == null)
				throw new ArgumentNullException("name");
			if (type == null) 
				throw new ArgumentNullException("type");

			this.tableInfo = tableInfo;
			this.name = name;
			this.type = type;
		}

		public DataTableInfo TableInfo {
			get { return tableInfo; }
			internal set { tableInfo = value; }
		}

		///<summary>
		///</summary>
		public string Name {
			get { return name; }
			internal set { name = value; }
		}


		///<summary>
		///</summary>
		public bool IsNotNull {
			get { return notNull; }
			set { notNull = value; }
		}

		public SqlType SqlType {
			get { return type.SQLType; }
		}

		///<summary>
		///</summary>
		public int Size {
			get { return (type is ISizeableType) ? ((ISizeableType)type).Size : -1; }
		}

		///<summary>
		///</summary>
		public int Scale {
			get { return (type is TNumericType) ? ((TNumericType)type).Scale : -1; }
		}

		/// <summary>
		/// Gets or sets the name of the scheme we use to index this column.
		/// </summary>
		/// <remarks>
		/// It will be either <i>InsertSearch</i> or <b>BlindSearch</b>.
		/// </remarks>
		public string IndexScheme {
			get {
				if (indexType.Equals(""))
					return "InsertSearch";
				return indexType;
			}
			set { indexType = value; }
		}

		/// <summary>
		/// Gets <b>true</b> if this type of column is able to be indexed,
		/// otherwise <b>false</b>.
		/// </summary>
		public bool IsIndexableType {
			get { return type.DbType != DbType.Blob && type.DbType != DbType.Object; }
		}

		///<summary>
		/// If this column represents an object, gets or sets the name of 
		/// the type the object must be derived from to be added to the
		/// column.
		///</summary>
		///<exception cref="ApplicationException"></exception>
		public string TypeConstraintString {
			get { return baseTypeConstraint; }
			set {
				baseTypeConstraint = value;
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
						baseType = Array.CreateInstance(ac, 0).GetType();
					} else {
						// Not an array
						baseType = Type.GetType(value, true, true);
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
			get { return baseType; }
		}

		/// <summary>
		/// Returns the TType for this column.
		/// </summary>
		public TType TType {
			get { return type; }
		}

		///<summary>
		///</summary>
		///<param name="expression"></param>
		public void SetDefaultExpression(Expression expression) {
			defaultExpressionString = expression.Text.ToString();
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
		///<param name="columnName"></param>
		///<returns></returns>
		internal ColumnDescription ColumnDescriptionValue(string columnName) {
			ColumnDescription field = new ColumnDescription(columnName, type.DbType, Size, IsNotNull);
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
			output.Write(" ");
			output.Write(type.ToSQLString());
		}


		// ---------- IO Methods ----------

		/// <summary>
		/// Writes this column information output to a <see cref="BinaryWriter"/>.
		/// </summary>
		/// <param name="output"></param>
		internal void Write(BinaryWriter output) {
			output.Write(3); // The version

			output.Write(name);
			TType.ToBinaryWriter(type, output);
			output.Write(notNull);

			if (defaultExpressionString != null) {
				output.Write(true);
				output.Write(defaultExpressionString);
				//new String(default_exp.text().toString()));
			} else {
				output.Write(false);
			}

			output.Write(indexType);
			output.Write(baseTypeConstraint); // Introduced input version 2.
		}

		/// <summary>
		/// Reads this column from a <see cref="BinaryReader"/>.
		/// </summary>
		/// <param name="tableInfo"></param>
		/// <param name="input"></param>
		/// <returns></returns>
		internal static DataTableColumnInfo Read(DataTableInfo tableInfo, BinaryReader input) {
			int ver = input.ReadInt32();

			string name = input.ReadString();
			TType type = TType.ReadFrom(input);
			DataTableColumnInfo cd = new DataTableColumnInfo(tableInfo, name, type);
			
			cd.notNull = input.ReadBoolean();

			bool hasExpression = input.ReadBoolean();
			if (hasExpression) {
				cd.defaultExpressionString = input.ReadString();
				//      cd.default_exp = Expression.Parse(input.readUTF());
			}

			cd.indexType = input.ReadString();
			if (ver > 1) {
				string cc = input.ReadString();
				if (!cc.Equals("")) {
					cd.TypeConstraintString = cc;
				}
			} else {
				cd.baseTypeConstraint = "";
			}

			return cd;
		}

		object ICloneable.Clone() {
			return Clone();
		}

		public DataTableColumnInfo Clone() {
			DataTableColumnInfo clone = new DataTableColumnInfo(tableInfo, (string)name.Clone(), type);
			clone.notNull = notNull;
			if (!String.IsNullOrEmpty(defaultExpressionString)) {
				clone.defaultExpressionString = (string) defaultExpressionString.Clone();
			}
			clone.indexType = (string)indexType.Clone();
			clone.baseTypeConstraint = (string)baseTypeConstraint.Clone();
			return clone;
		}
	}
}