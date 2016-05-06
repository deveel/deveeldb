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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Util;

namespace Deveel.Data.Transactions {
	class TransactionTableContainer : ITableContainer {
		private readonly ITransaction transaction;
		private readonly TableInfo[] tableInfos;

		private static readonly TableInfo[] IntTableInfo;

		public TransactionTableContainer(ITransaction transaction) {
			this.transaction = transaction;
			tableInfos = IntTableInfo;
		}

		static TransactionTableContainer() {
			// SYSTEM.TABLE_INFO
			TableInfoTableInfo = new TableInfo(SystemSchema.TableInfoTableName);
			TableInfoTableInfo.AddColumn("catalog", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("schema", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("name", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("type", PrimitiveTypes.String());
			TableInfoTableInfo.AddColumn("other", PrimitiveTypes.String());
			TableInfoTableInfo = TableInfoTableInfo.AsReadOnly();

			// SYSTEM.TABLE_COLUMNS
			TableColumnsTableInfo = new TableInfo(SystemSchema.TableColumnsTableName);
			TableColumnsTableInfo.AddColumn("schema", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("table", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("column", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("sql_type", PrimitiveTypes.Numeric());
			TableColumnsTableInfo.AddColumn("type_desc", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("size", PrimitiveTypes.Numeric());
			TableColumnsTableInfo.AddColumn("scale", PrimitiveTypes.Numeric());
			TableColumnsTableInfo.AddColumn("not_null", PrimitiveTypes.Boolean());
			TableColumnsTableInfo.AddColumn("default", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("index_str", PrimitiveTypes.String());
			TableColumnsTableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric());
			TableColumnsTableInfo = TableColumnsTableInfo.AsReadOnly();

			// SYSTEM.VARIABLES
			VariablesTableInfo = new TableInfo(SystemSchema.VariablesTableName);
			VariablesTableInfo.AddColumn("var", PrimitiveTypes.String());
			VariablesTableInfo.AddColumn("type", PrimitiveTypes.String());
			VariablesTableInfo.AddColumn("value", PrimitiveTypes.String());
			VariablesTableInfo.AddColumn("constant", PrimitiveTypes.Boolean());
			VariablesTableInfo.AddColumn("not_null", PrimitiveTypes.Boolean());
			VariablesTableInfo.AddColumn("is_set", PrimitiveTypes.Boolean());
			VariablesTableInfo = VariablesTableInfo.AsReadOnly();

			// SYSTEM.PRODUCT_INFO
			ProductInfoTableInfo = new TableInfo(SystemSchema.ProductInfoTableName);
			ProductInfoTableInfo.AddColumn("var", PrimitiveTypes.String());
			ProductInfoTableInfo.AddColumn("value", PrimitiveTypes.String());
			ProductInfoTableInfo = ProductInfoTableInfo.AsReadOnly();

			// SYSTEM.SQL_TYPES
			SqlTypesTableInfo = new TableInfo(SystemSchema.SqlTypesTableName);
			SqlTypesTableInfo.AddColumn("TYPE_NAME", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("DATA_TYPE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("PRECISION", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("LITERAL_PREFIX", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("LITERAL_SUFFIX", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("CREATE_PARAMS", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("NULLABLE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("CASE_SENSITIVE", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("SEARCHABLE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("UNSIGNED_ATTRIBUTE", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("FIXED_PREC_SCALE", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("AUTO_INCREMENT", PrimitiveTypes.Boolean());
			SqlTypesTableInfo.AddColumn("LOCAL_TYPE_NAME", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("MINIMUM_SCALE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("MAXIMUM_SCALE", PrimitiveTypes.Numeric());
			SqlTypesTableInfo.AddColumn("SQL_DATA_TYPE", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("SQL_DATETIME_SUB", PrimitiveTypes.String());
			SqlTypesTableInfo.AddColumn("NUM_PREC_RADIX", PrimitiveTypes.Numeric());
			SqlTypesTableInfo = SqlTypesTableInfo.AsReadOnly();

			// SYSTEM.PRIVS
			PrivilegesTableInfo = new TableInfo(SystemSchema.PrivilegesTableName);
			PrivilegesTableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric());
			PrivilegesTableInfo.AddColumn("description", PrimitiveTypes.String());
			PrivilegesTableInfo = PrivilegesTableInfo.AsReadOnly();

			IntTableInfo = new TableInfo[6];
			IntTableInfo[0] = TableInfoTableInfo;
			IntTableInfo[1] = TableColumnsTableInfo;
			IntTableInfo[2] = ProductInfoTableInfo;
			IntTableInfo[3] = VariablesTableInfo;
			IntTableInfo[4] = SqlTypesTableInfo;
			IntTableInfo[5] = PrivilegesTableInfo;
		}

		private static readonly TableInfo TableInfoTableInfo;

		private static readonly TableInfo TableColumnsTableInfo;

		private static readonly TableInfo SqlTypesTableInfo;

		private static readonly TableInfo VariablesTableInfo;

		private static readonly TableInfo ProductInfoTableInfo;

		private static readonly TableInfo PrivilegesTableInfo;

		public int TableCount {
			get { return tableInfos.Length; }
		}

		public int FindByName(ObjectName name) {
			var ignoreCase = transaction.IgnoreIdentifiersCase();
			for (int i = 0; i < tableInfos.Length; i++) {
				var info = tableInfos[i];
				if (info != null && 
				    info.TableName.Equals(name, ignoreCase))
					return i;
			}

			return -1;
		}

		public ObjectName GetTableName(int offset) {
			if (offset < 0 || offset >= tableInfos.Length)
				throw new ArgumentOutOfRangeException("offset");

			return tableInfos[offset].TableName;
		}

		public TableInfo GetTableInfo(int offset) {
			if (offset < 0 || offset >= tableInfos.Length)
				throw new ArgumentOutOfRangeException("offset");

			return tableInfos[offset];
		}

		public string GetTableType(int offset) {
			return TableTypes.SystemTable;
		}

		public bool ContainsTable(ObjectName name) {
			return FindByName(name) >= 0;
		}

		public ITable GetTable(int offset) {
			if (offset == 0)
				return new TableInfoTable(transaction);
			if (offset == 1)
				return new TableColumnsTable(transaction);
			if (offset == 2)
				return new ProductInfoTable(transaction);
			if (offset == 3)
				return new VariablesTable(transaction);
			if (offset == 4)
				return new SqlTypesTable(transaction);
			if (offset == 5)
				return new PrivilegesTable(transaction);

			throw new ArgumentOutOfRangeException("offset");
		}

		#region TableInfoTable

		class TableInfoTable : GeneratedTable {
			private List<TableInfoObject> tableInfoObjects;
			private int rowCount;

			public TableInfoTable(ITransaction transaction)
				: base(transaction.Database.Context) {
				Transaction = transaction;
				tableInfoObjects = new List<TableInfoObject>();

				Init();
			}

			public ITransaction Transaction { get; private set; }

			public override TableInfo TableInfo {
				get { return TableInfoTableInfo; }
			}

			public override int RowCount {
				get { return rowCount; }
			}

			private void Init() {
				// All the tables
				var manager = Transaction.GetTableManager();
				var tableNames = manager.GetTableNames();

				var tableList = tableNames.ToArray();
				Array.Sort(tableList);
				rowCount = tableList.Length;

				foreach (var tableName in tableList) {
					string curType = Transaction.GetTableType(tableName);

					// If the table is in the SYSTEM schema, the type is defined as a
					// SYSTEM TABLE.
					if (curType.Equals("TABLE") &&
						tableName.Parent.Name.Equals(SystemSchema.Name)) {
						curType = "SYSTEM TABLE";
					}

					tableInfoObjects.Add(new TableInfoObject(null, tableName.Parent.Name, tableName.Name, curType, null));
				}
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= tableInfoObjects.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var tableInfo = tableInfoObjects[(int)rowNumber];

				switch (columnOffset) {
					case 0:
						return Field.String(tableInfo.Catalog);
					case 1:
						return Field.String(tableInfo.Schema);
					case 2:
						return Field.String(tableInfo.Name);
					case 3:
						return Field.String(tableInfo.Type);
					case 4:
						return Field.String(tableInfo.Comments);
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}

			#region TableInfoObject

			class TableInfoObject {
				public TableInfoObject(string catalog, string schema, string name, string type, string comments) {
					Catalog = catalog;
					Schema = schema;
					Name = name;
					Type = type;
					Comments = comments;
				}

				public string Name { get; private set; }
				public string Schema { get; private set; }
				public string Catalog { get; private set; }
				public string Type { get; private set; }
				public string Comments { get; private set; }
			}
			#endregion
		}

		#endregion

		#region TableColumnsTable

		class TableColumnsTable : GeneratedTable {
			private ITransaction transaction;

			public TableColumnsTable(ITransaction transaction)
				: base(transaction.Database.Context) {
				this.transaction = transaction;
			}

			public override TableInfo TableInfo {
				get { return TableColumnsTableInfo; }
			}

			public override int RowCount {
				get { return GetRowCount(); }
			}

			private int GetRowCount() {
				// All the tables
				var tableManager = transaction.GetTableManager();
				var list = tableManager.GetTableNames();

				int colCount = 0;
				foreach (var tableName in list) {
					var info = tableManager.GetTableInfo(tableName);
					if (info == null)
						throw new InvalidOperationException(String.Format("Table information not found for '{0}'.", tableName));

					colCount += info.ColumnCount;
				}

				return colCount;
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				// All the tables
				var tableManager = transaction.GetTableManager();
				var list = tableManager.GetTableNames();
				var visibleTables = list.Select(name => transaction.GetTableInfo(name));

				int rs = 0;
				foreach (var info in visibleTables) {
					var schemaName = info.SchemaName == null ? null : info.SchemaName.FullName;

					int b = rs;
					rs += info.ColumnCount;
					if (rowNumber >= b && rowNumber < rs) {
						// This is the column that was requested,
						var seqNo = rowNumber - b;
						var colInfo = info[(int)seqNo];

						var defaultExpression = colInfo.HasDefaultExpression ? colInfo.DefaultExpression.ToString() : null;

						switch (columnOffset) {
							case 0:  // schema
								return GetColumnValue(columnOffset, new SqlString(schemaName));
							case 1:  // table
								return GetColumnValue(columnOffset, new SqlString(info.Name));
							case 2:  // column
								return GetColumnValue(columnOffset, new SqlString(colInfo.ColumnName));
							case 3:  // sql_type
								return GetColumnValue(columnOffset, new SqlNumber((int)colInfo.ColumnType.TypeCode));
							case 4:  // type_desc
								return GetColumnValue(columnOffset, new SqlString(colInfo.ColumnType.ToString()));
							case 5:  // size
								return GetColumnValue(columnOffset, new SqlNumber(colInfo.Size));
							case 6:  // scale
								return GetColumnValue(columnOffset, new SqlNumber(colInfo.Scale));
							case 7:  // not_null
								return GetColumnValue(columnOffset, (SqlBoolean)colInfo.IsNotNull);
							case 8:  // default
								return GetColumnValue(columnOffset, new SqlString(defaultExpression));
							case 9:  // index_str
								return GetColumnValue(columnOffset, new SqlString(colInfo.IndexType));
							case 10:  // seq_no
								return GetColumnValue(columnOffset, new SqlNumber(seqNo));
							default:
								throw new ArgumentOutOfRangeException("columnOffset");
						}
					}

				}  // for each visible table

				throw new ArgumentOutOfRangeException("rowNumber", "Row out of bounds.");
			}

			protected override void Dispose(bool disposing) {
				transaction = null;
				base.Dispose(disposing);
			}
		}

		#endregion

		#region SqlTypesTable

		class SqlTypesTable : GeneratedTable {
			private ITransaction transaction;
			private List<SqlTypeInfo> sqlTypes;

			public SqlTypesTable(ITransaction transaction)
				: base(transaction.Database.Context) {
				this.transaction = transaction;

				sqlTypes = new List<SqlTypeInfo>();

				Init();
			}

			public override TableInfo TableInfo {
				get { return SqlTypesTableInfo; }
			}

			public override int RowCount {
				get { return sqlTypes.Count; }
			}

			private void AddType(string name, string localName, SqlTypeCode type, byte precision, string prefix, string suffix, bool searchable) {
				sqlTypes.Add(new SqlTypeInfo {
					TypeName = name,
					LocalName = localName,
					Type = type,
					Precision = precision,
					LiteralPrefix = prefix,
					LiteralSuffix = suffix,
					Searchable = (byte)(searchable ? 3 : 0)
				});
			}

			private void Init() {
				AddType("BIT", "BOOLEAN", SqlTypeCode.Bit, 1, null, null, true);
				AddType("BOOLEAN", "BOOLEAN", SqlTypeCode.Boolean, 1, null, null, true);
				AddType("TINYINT", "NUMBER", SqlTypeCode.TinyInt, 9, null, null, true);
				AddType("SMALLINT", "NUMBER", SqlTypeCode.SmallInt, 9, null, null, true);
				AddType("INTEGER", "NUMBER", SqlTypeCode.Integer, 9, null, null, true);
				AddType("BIGINT", "NUMBER", SqlTypeCode.BigInt, 9, null, null, true);
				AddType("FLOAT", "NUMBER", SqlTypeCode.Float, 9, null, null, true);
				AddType("REAL", "NUMBER", SqlTypeCode.Real, 9, null, null, true);
				AddType("DOUBLE", "NUMBER", SqlTypeCode.Double, 9, null, null, true);
				AddType("NUMERIC", "NUMBER", SqlTypeCode.Numeric, 9, null, null, true);
				AddType("DECIMAL", "NUMBER", SqlTypeCode.Decimal, 9, null, null, true);
				AddType("CHAR", "STRING", SqlTypeCode.Char, 9, "'", "'", true);
				AddType("VARCHAR", "STRING", SqlTypeCode.VarChar, 9, "'", "'", true);
				AddType("LONGVARCHAR", "STRING", SqlTypeCode.LongVarChar, 9, "'", "'", true);
				AddType("DATE", "DATETIME", SqlTypeCode.Date, 9, null, null, true);
				AddType("TIME", "DATETIME", SqlTypeCode.Time, 9, null, null, true);
				AddType("TIMESTAMP", "DATETIME", SqlTypeCode.TimeStamp, 9, null, null, true);
				AddType("BINARY", "BINARY", SqlTypeCode.Binary, 9, null, null, false);
				AddType("VARBINARY", "BINARY", SqlTypeCode.VarBinary, 9, null, null, false);
				AddType("LONGVARBINARY", "BINARY", SqlTypeCode.LongVarBinary, 9, null, null, false);
				AddType("OBJECT", "OBJECT", SqlTypeCode.Object, 9, null, null, false);
				AddType("TYPE", "TYPE", SqlTypeCode.Type, 9, null, null, false);
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				// TODO: handle also the user-types here?

				if (rowNumber < 0 || rowNumber >= sqlTypes.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var typeInfo = sqlTypes[(int)rowNumber];
				switch (columnOffset) {
					case 0:  // type_name
						return GetColumnValue(columnOffset, new SqlString(typeInfo.TypeName));
					case 1:  // data_type
						return GetColumnValue(columnOffset, new SqlNumber((int)typeInfo.Type));
					case 2:  // precision
						return GetColumnValue(columnOffset, new SqlNumber(typeInfo.Precision));
					case 3:  // literal_prefix
						return GetColumnValue(columnOffset, new SqlString(typeInfo.LiteralPrefix));
					case 4:  // literal_suffix
						return GetColumnValue(columnOffset, new SqlString(typeInfo.LiteralSuffix));
					case 5:  // create_params
						return GetColumnValue(columnOffset, SqlString.Null);
					case 6:  // nullable
						return GetColumnValue(columnOffset, SqlNumber.One);
					case 7:  // case_sensitive
						return GetColumnValue(columnOffset, SqlBoolean.True);
					case 8:  // searchable
						return GetColumnValue(columnOffset, new SqlNumber(typeInfo.Searchable));
					case 9:  // unsigned_attribute
						return GetColumnValue(columnOffset, SqlBoolean.False);
					case 10:  // fixed_prec_scale
						return GetColumnValue(columnOffset, SqlBoolean.False);
					case 11:  // auto_increment
						return GetColumnValue(columnOffset, SqlBoolean.False);
					case 12:  // local_type_name
						return GetColumnValue(columnOffset, new SqlString(typeInfo.LocalName));
					case 13:  // minimum_scale
						return GetColumnValue(columnOffset, SqlNumber.Zero);
					case 14:  // maximum_scale
						return GetColumnValue(columnOffset, new SqlNumber(10000000));
					case 15:  // sql_data_type
						return GetColumnValue(columnOffset, SqlNull.Value);
					case 16:  // sql_datetype_sub
						return GetColumnValue(columnOffset, SqlNull.Value);
					case 17:  // num_prec_radix
						return GetColumnValue(columnOffset, new SqlNumber(10));
					default:
						throw new ArgumentOutOfRangeException("columnOffset");

				}
			}

			protected override void Dispose(bool disposing) {
				transaction = null;
				sqlTypes = null;

				base.Dispose(disposing);
			}

			#region SqlTypeInfo

			class SqlTypeInfo {
				public string TypeName;
				public string LocalName;
				public SqlTypeCode Type;
				public byte Precision;
				public string LiteralPrefix;
				public string LiteralSuffix;
				public byte Searchable;
			}

			#endregion
		}

		#endregion

		#region ProductInfoTable

		class ProductInfoTable : GeneratedTable {
			private List<ISqlString> keyValuePairs;

			public ProductInfoTable(ITransaction transaction)
				: base(transaction.Database.Context) {
				Init();
			}

			public override TableInfo TableInfo {
				get { return ProductInfoTableInfo; }
			}

			public override int RowCount {
				get { return keyValuePairs.Count / 2; }
			}

			private void Init() {
				keyValuePairs = new List<ISqlString>();

				var productInfo = ProductInfo.Current;

				// Set up the product variables.
				keyValuePairs.Add(new SqlString("title"));
				keyValuePairs.Add(new SqlString(productInfo.Title));

				keyValuePairs.Add(new SqlString("version"));
				keyValuePairs.Add(productInfo.Version != null ? new SqlString(productInfo.Version.ToString()) : SqlString.Null);

				keyValuePairs.Add(new SqlString("dataVersion"));
				keyValuePairs.Add(productInfo.DataVersion != null ? new SqlString(productInfo.DataVersion.ToString()) : SqlString.Null);

				keyValuePairs.Add(new SqlString("fileVersion"));
				keyValuePairs.Add(productInfo.FileVersion != null ? new SqlString(productInfo.FileVersion.ToString()) : SqlString.Null);

				keyValuePairs.Add(new SqlString("copyright"));
				keyValuePairs.Add(new SqlString(productInfo.Copyright));

				keyValuePairs.Add(new SqlString("description"));
				keyValuePairs.Add(new SqlString(productInfo.Description));

				keyValuePairs.Add(new SqlString("company"));
				keyValuePairs.Add(new SqlString(productInfo.Company));
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				switch (columnOffset) {
					case 0:  // var
						return GetColumnValue(columnOffset, keyValuePairs[(int)rowNumber * 2]);
					case 1:  // value
						return GetColumnValue(columnOffset, keyValuePairs[(int)(rowNumber * 2) + 1]);
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}
		}

		#endregion

		#region VariablesTable

		class VariablesTable : GeneratedTable {
			private ITransaction transaction;
			private List<Variable> variables;

			public VariablesTable(ITransaction transaction)
				: base(transaction.Database.Context) {
				this.transaction = transaction;
				Init();
			}

			public override TableInfo TableInfo {
				get { return VariablesTableInfo; }
			}

			public override int RowCount {
				get { return variables.Count; }
			}

			private void Init() {
				lock (transaction.Database) {
					variables = new List<Variable>();

					var context = (IContext) transaction.Context;
					while (context != null) {
						if (context is IVariableScope) {
							var vars = ((IVariableScope) context).VariableManager.ToList();
							variables.AddRange(vars);
						}

						context = context.Parent;
					}
				}
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber > variables.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var variable = variables[(int) rowNumber];

				switch (columnOffset) {
					case 0:
						return GetColumnValue(columnOffset, new SqlString(variable.Name));
					case 1:
						return GetColumnValue(columnOffset, new SqlString(variable.Type.ToString()));
					case 2:
						return GetColumnValue(columnOffset,
							variable.Expression != null ? new SqlString(variable.Expression.ToString()) : SqlString.Null);
					case 3:
						return GetColumnValue(columnOffset, new SqlBoolean(variable.IsConstant));
					case 4:
						return GetColumnValue(columnOffset, new SqlBoolean(variable.IsNotNull));
					case 5:
						return GetColumnValue(columnOffset, new SqlBoolean(variable.Expression != null));
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}

			protected override void Dispose(bool disposing) {
				if (disposing) {
					if (variables != null)
						variables.Clear();
				}

				variables = null;
				transaction = null;
				base.Dispose(disposing);
			}
		}

		#endregion

		#region PrivilegesTable

		class PrivilegesTable : GeneratedTable {
			private readonly IList<KeyValuePair<string, int>> privBits;

			public PrivilegesTable(ITransaction transaction)
				: base(transaction.Database.Context) {
				privBits = FormPrivilegesValues();
			}

			private IList<KeyValuePair<string, int>> FormPrivilegesValues() {
				var names = Enum.GetNames(typeof(Privileges));
				var values = Enum.GetValues(typeof(Privileges));

				return names
					.Select((t, i) => new KeyValuePair<string, int>(t, (int) values.GetValue(i))).ToList();
			}

			public override TableInfo TableInfo {
				get { return PrivilegesTableInfo; }
			}

			public override int RowCount {
				get { return privBits.Count; }
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= privBits.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var pair = privBits[(int)rowNumber];
				switch (columnOffset) {
					case 0:
						return Field.Integer(pair.Value);
					case 1:
						return Field.VarChar(pair.Key.ToUpperInvariant());
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}
		}

		#endregion
	}
}