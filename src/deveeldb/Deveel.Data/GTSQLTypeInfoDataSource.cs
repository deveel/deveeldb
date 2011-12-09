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
using System.Collections;

using Deveel.Math;

namespace Deveel.Data {
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
		private ArrayList key_value_pairs;

		/// <summary>
		/// Constant for type_nullable types.
		/// </summary>
		private static readonly BigNumber TYPE_NULLABLE = 1;

		public GTSQLTypeInfoDataSource(DatabaseConnection connection)
			: base(connection.System) {
			database = connection;
			key_value_pairs = new ArrayList();
		}

		/// <summary>
		/// Adds a type description.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="precision"></param>
		/// <param name="prefix"></param>
		/// <param name="suffix"></param>
		/// <param name="oops"></param>
		/// <param name="searchable"></param>
		private void AddType(String name, SqlType type, int precision,
							 String prefix, String suffix, String oops,
							 bool searchable) {
			key_value_pairs.Add(name);
			key_value_pairs.Add((BigNumber)(int)type);
			key_value_pairs.Add((BigNumber)precision);
			key_value_pairs.Add(prefix);
			key_value_pairs.Add(suffix);
			key_value_pairs.Add(searchable ? (BigNumber)3 : (BigNumber)0);
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTSQLTypeInfoDataSource Init() {
			AddType("BIT", SqlType.Bit, 1, null, null, null, true);
			AddType("BOOLEAN", SqlType.Bit, 1, null, null, null, true);
			AddType("TINYINT", SqlType.TinyInt, 9, null, null, null, true);
			AddType("SMALLINT", SqlType.SmallInt, 9, null, null, null, true);
			AddType("INTEGER", SqlType.Integer, 9, null, null, null, true);
			AddType("BIGINT", SqlType.BigInt, 9, null, null, null, true);
			AddType("FLOAT", SqlType.Float, 9, null, null, null, true);
			AddType("REAL", SqlType.Real, 9, null, null, null, true);
			AddType("DOUBLE", SqlType.Double, 9, null, null, null, true);
			AddType("NUMERIC", SqlType.Numeric, 9, null, null, null, true);
			AddType("DECIMAL", SqlType.Decimal, 9, null, null, null, true);
			AddType("IDENTITY", SqlType.Identity, 9, null, null, null, true);
			AddType("CHAR", SqlType.Char, 9, "'", "'", null, true);
			AddType("VARCHAR", SqlType.VarChar, 9, "'", "'", null, true);
			AddType("LONGVARCHAR", SqlType.LongVarChar, 9, "'", "'", null, true);
			AddType("DATE", SqlType.Date, 9, null, null, null, true);
			AddType("TIME", SqlType.Time, 9, null, null, null, true);
			AddType("TIMESTAMP", SqlType.TimeStamp, 9, null, null, null, true);
			AddType("BINARY", SqlType.Binary, 9, null, null, null, false);
			AddType("VARBINARY", SqlType.VarBinary, 9, null, null, null, false);
			AddType("LONGVARBINARY", SqlType.LongVarBinary, 9, null, null, null, false);
			AddType("OBJECT", SqlType.Object, 9, null, null, null, false);

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo TableInfo {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return key_value_pairs.Count/6; }
		}

		public override TObject GetCellContents(int column, int row) {
			int i = (row * 6);
			switch (column) {
				case 0:  // type_name
					return GetColumnValue(column, key_value_pairs[i]);
				case 1:  // data_type
					return GetColumnValue(column, key_value_pairs[i + 1]);
				case 2:  // precision
					return GetColumnValue(column, key_value_pairs[i + 2]);
				case 3:  // literal_prefix
					return GetColumnValue(column, key_value_pairs[i + 3]);
				case 4:  // literal_suffix
					return GetColumnValue(column, key_value_pairs[i + 4]);
				case 5:  // create_params
					return GetColumnValue(column, null);
				case 6:  // nullable
					return GetColumnValue(column, TYPE_NULLABLE);
				case 7:  // case_sensitive
					return GetColumnValue(column, true);
				case 8:  // searchable
					return GetColumnValue(column, key_value_pairs[i + 5]);
				case 9:  // unsigned_attribute
					return GetColumnValue(column, false);
				case 10:  // fixed_prec_scale
					return GetColumnValue(column, false);
				case 11:  // auto_increment
					return GetColumnValue(column, (SqlType)key_value_pairs[i + 1] == SqlType.Identity);
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
				key_value_pairs = null;
				database = null;
			}
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo DEF_DATA_TABLE_DEF;

		static GTSQLTypeInfoDataSource() {

			DataTableInfo info = new DataTableInfo();
			info.TableName = new TableName(Database.SystemSchema, "sql_types");

			// Add column definitions
			info.AddColumn(GetStringColumn("TYPE_NAME"));
			info.AddColumn(GetNumericColumn("DATA_TYPE"));
			info.AddColumn(GetNumericColumn("PRECISION"));
			info.AddColumn(GetStringColumn("LITERAL_PREFIX"));
			info.AddColumn(GetStringColumn("LITERAL_SUFFIX"));
			info.AddColumn(GetStringColumn("CREATE_PARAMS"));
			info.AddColumn(GetNumericColumn("NULLABLE"));
			info.AddColumn(GetBooleanColumn("CASE_SENSITIVE"));
			info.AddColumn(GetNumericColumn("SEARCHABLE"));
			info.AddColumn(GetBooleanColumn("UNSIGNED_ATTRIBUTE"));
			info.AddColumn(GetBooleanColumn("FIXED_PREC_SCALE"));
			info.AddColumn(GetBooleanColumn("AUTO_INCREMENT"));
			info.AddColumn(GetStringColumn("LOCAL_TYPE_NAME"));
			info.AddColumn(GetNumericColumn("MINIMUM_SCALE"));
			info.AddColumn(GetNumericColumn("MAXIMUM_SCALE"));
			info.AddColumn(GetStringColumn("SQL_DATA_TYPE"));
			info.AddColumn(GetStringColumn("SQL_DATETIME_SUB"));
			info.AddColumn(GetNumericColumn("NUM_PREC_RADIX"));

			// Set to immutable
			info.SetImmutable();

			DEF_DATA_TABLE_DEF = info;
		}
	}
}