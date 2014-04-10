// 
//  Copyright 2010-2013  Deveel
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
using System.Collections.Generic;

using Deveel.Data.Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// A <see cref="GTDataSource"/> that models all SQL types available.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> This is not designed to be a long kept object. It must 
	/// not last beyond the lifetime of a transaction.
	/// </remarks>
	internal class GTSQLTypeInfoDataSource : GTDataSource {
		/// <summary>
		/// The DatabaseConnection object.  Currently this is not used, but it may
		/// be needed in the future if user-defined SQL types are supported.
		/// </summary>
		private DatabaseConnection database;

		/// <summary>
		/// The list of info keys/values in this object.
		/// </summary>
		private List<SqlTypeInfo> sqlTypes;

		/// <summary>
		/// Constant for type_nullable types.
		/// </summary>
		private static readonly BigNumber TypeNullable = 1;

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo DataTableInfo;

		static GTSQLTypeInfoDataSource() {

			DataTableInfo info = new DataTableInfo(new TableName(SystemSchema.Name, "sql_types"));

			// Add column definitions
			info.AddColumn("TYPE_NAME", PrimitiveTypes.VarString);
			info.AddColumn("DATA_TYPE", PrimitiveTypes.Numeric);
			info.AddColumn("PRECISION", PrimitiveTypes.Numeric);
			info.AddColumn("LITERAL_PREFIX", PrimitiveTypes.VarString);
			info.AddColumn("LITERAL_SUFFIX", PrimitiveTypes.VarString);
			info.AddColumn("CREATE_PARAMS", PrimitiveTypes.VarString);
			info.AddColumn("NULLABLE", PrimitiveTypes.Numeric);
			info.AddColumn("CASE_SENSITIVE", PrimitiveTypes.Boolean);
			info.AddColumn("SEARCHABLE", PrimitiveTypes.Numeric);
			info.AddColumn("UNSIGNED_ATTRIBUTE", PrimitiveTypes.Boolean);
			info.AddColumn("FIXED_PREC_SCALE", PrimitiveTypes.Boolean);
			info.AddColumn("AUTO_INCREMENT", PrimitiveTypes.Boolean);
			info.AddColumn("LOCAL_TYPE_NAME", PrimitiveTypes.VarString);
			info.AddColumn("MINIMUM_SCALE", PrimitiveTypes.Numeric);
			info.AddColumn("MAXIMUM_SCALE", PrimitiveTypes.Numeric);
			info.AddColumn("SQL_DATA_TYPE", PrimitiveTypes.VarString);
			info.AddColumn("SQL_DATETIME_SUB", PrimitiveTypes.VarString);
			info.AddColumn("NUM_PREC_RADIX", PrimitiveTypes.Numeric);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;
		}

		public GTSQLTypeInfoDataSource(DatabaseConnection connection)
			: base(connection.Context) {
			database = connection;
			sqlTypes = new List<SqlTypeInfo>();
		}

		/// <summary>
		/// Adds a type description.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="precision"></param>
		/// <param name="prefix"></param>
		/// <param name="suffix"></param>
		/// <param name="searchable"></param>
		private void AddType(string name, SqlType type, byte precision, string prefix, string suffix, bool searchable) {
			SqlTypeInfo typeInfo = new SqlTypeInfo();
			typeInfo.Name = name;
			typeInfo.Type = type;
			typeInfo.Precision = precision;
			typeInfo.LiteralPrefix = prefix;
			typeInfo.LiteralSuffix = suffix;
			typeInfo.Searchable = (byte)(searchable ? 3 : 0);
			sqlTypes.Add(typeInfo);
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTSQLTypeInfoDataSource Init() {
			AddType("BIT", SqlType.Bit, 1, null, null, true);
			AddType("BOOLEAN", SqlType.Bit, 1, null, null, true);
			AddType("TINYINT", SqlType.TinyInt, 9, null, null, true);
			AddType("SMALLINT", SqlType.SmallInt, 9, null, null, true);
			AddType("INTEGER", SqlType.Integer, 9, null, null, true);
			AddType("BIGINT", SqlType.BigInt, 9, null, null, true);
			AddType("FLOAT", SqlType.Float, 9, null, null, true);
			AddType("REAL", SqlType.Real, 9, null, null, true);
			AddType("DOUBLE", SqlType.Double, 9, null, null, true);
			AddType("NUMERIC", SqlType.Numeric, 9, null, null, true);
			AddType("DECIMAL", SqlType.Decimal, 9, null, null, true);
			AddType("IDENTITY", SqlType.Identity, 9, null, null, true);
			AddType("CHAR", SqlType.Char, 9, "'", "'", true);
			AddType("VARCHAR", SqlType.VarChar, 9, "'", "'", true);
			AddType("LONGVARCHAR", SqlType.LongVarChar, 9, "'", "'", true);
			AddType("DATE", SqlType.Date, 9, null, null, true);
			AddType("TIME", SqlType.Time, 9, null, null, true);
			AddType("TIMESTAMP", SqlType.TimeStamp, 9, null, null, true);
			AddType("BINARY", SqlType.Binary, 9, null, null, false);
			AddType("VARBINARY", SqlType.VarBinary, 9, null, null, false);
			AddType("LONGVARBINARY", SqlType.LongVarBinary, 9, null, null, false);
			AddType("OBJECT", SqlType.Object, 9, null, null, false);

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		public override int RowCount {
			get { return sqlTypes.Count/6; }
		}

		public override TObject GetCell(int column, int row) {
			int i = (row * 6);
			SqlTypeInfo typeInfo = sqlTypes[row];

			switch (column) {
				case 0:  // type_name
					return GetColumnValue(column, typeInfo.Name);
				case 1:  // data_type
					return GetColumnValue(column, (int) typeInfo.Type);
				case 2:  // precision
					return GetColumnValue(column, typeInfo.Precision);
				case 3:  // literal_prefix
					return GetColumnValue(column, typeInfo.LiteralPrefix);
				case 4:  // literal_suffix
					return GetColumnValue(column, typeInfo.LiteralSuffix);
				case 5:  // create_params
					return GetColumnValue(column, null);
				case 6:  // nullable
					return GetColumnValue(column, TypeNullable);
				case 7:  // case_sensitive
					return GetColumnValue(column, true);
				case 8:  // searchable
					return GetColumnValue(column, typeInfo.Searchable);
				case 9:  // unsigned_attribute
					return GetColumnValue(column, false);
				case 10:  // fixed_prec_scale
					return GetColumnValue(column, false);
				case 11:  // auto_increment
					return GetColumnValue(column, typeInfo.Type == SqlType.Identity);
				case 12:  // local_type_name
					return GetColumnValue(column, null);
				case 13:  // minimum_scale
					return GetColumnValue(column, (BigNumber)0);
				case 14:  // maximum_scale
					return GetColumnValue(column, (BigNumber)10000000);
				case 15:  // sql_data_type
					return GetColumnValue(column, null);
				case 16:  // sql_datetype_sub
					return GetColumnValue(column, null);
				case 17:  // num_prec_radix
					return GetColumnValue(column, (BigNumber)10);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose(bool disposing) {
			if (disposing) {
				sqlTypes = null;
				database = null;
			}
		}

		#region SqlTypeInfo

		class SqlTypeInfo {
			public string Name;
			public SqlType Type;
			public byte Precision;
			public string LiteralPrefix;
			public string LiteralSuffix;
			public byte Searchable;

		}

		#endregion
	}
}