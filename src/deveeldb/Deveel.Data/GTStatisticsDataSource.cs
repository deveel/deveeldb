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

using Deveel.Data.Deveel.Data;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that 
	/// presents database statistical information.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> This is not designed to be a long kept object. It must not last
	/// beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTStatisticsDataSource : GTDataSource {
		/// <summary>
		/// Contains all the statistics information for this session.
		/// </summary>
		private String[] statistics_info;
		/// <summary>
		/// The system database stats.
		/// </summary>
		private Stats stats;

		public GTStatisticsDataSource(DatabaseConnection connection)
			: base(connection.System) {
			stats = connection.Database.Stats;
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTStatisticsDataSource Init() {

			lock (stats) {
				/*
				TODO: check this...
				stats.set((int)(Runtime.getRuntime().freeMemory() / 1024),
															  "Runtime.memory.freeKB");
				stats.set((int)(Runtime.getRuntime().totalMemory() / 1024),
															  "Runtime.memory.totalKB");
				*/
				String[] key_set = stats.Keys;
				int glob_length = key_set.Length * 2;
				statistics_info = new String[glob_length];

				for (int i = 0; i < glob_length; i += 2) {
					String key_name = key_set[i / 2];
					statistics_info[i] = key_name;
					statistics_info[i + 1] = stats.StatString(key_name);
				}

			}
			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo TableInfo {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return statistics_info.Length/2; }
		}

		public override TObject GetCellContents(int column, int row) {
			switch (column) {
				case 0:  // stat_name
					return GetColumnValue(column, statistics_info[row * 2]);
				case 1:  // value
					return GetColumnValue(column, statistics_info[(row * 2) + 1]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose(bool disposing) {
			statistics_info = null;
			stats = null;
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo DEF_DATA_TABLE_DEF;

		static GTStatisticsDataSource() {

			DataTableInfo info = new DataTableInfo(new TableName(SystemSchema.Name, "database_stats"));

			// Add column definitions
			info.AddColumn("stat_name", TType.StringType);
			info.AddColumn("value", TType.StringType);

			// Set to immutable
			info.IsReadOnly = true;

			DEF_DATA_TABLE_DEF = info;
		}
	}
}