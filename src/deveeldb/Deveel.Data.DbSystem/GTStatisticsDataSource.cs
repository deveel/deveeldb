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

using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
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
		private string[] statsInfo;
		/// <summary>
		/// The system database stats.
		/// </summary>
		private Stats stats;

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo DataTableInfo;

		static GTStatisticsDataSource() {
			DataTableInfo info = new DataTableInfo(SystemSchema.DatabaseStatistics);

			// Add column definitions
			info.AddColumn("stat_name", TType.StringType);
			info.AddColumn("value", TType.StringType);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;
		}

		public GTStatisticsDataSource(DatabaseConnection connection)
			: base(connection.Context) {
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
				string[] keySet = stats.Keys;
				int globLength = keySet.Length * 2;
				statsInfo = new string[globLength];

				for (int i = 0; i < globLength; i += 2) {
					string keyName = keySet[i / 2];
					statsInfo[i] = keyName;
					statsInfo[i + 1] = stats.StatString(keyName);
				}

			}

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		public override int RowCount {
			get { return statsInfo.Length/2; }
		}

		public override TObject GetCell(int column, int row) {
			switch (column) {
				case 0:  // stat_name
					return GetColumnValue(column, statsInfo[row * 2]);
				case 1:  // value
					return GetColumnValue(column, statsInfo[(row * 2) + 1]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose(bool disposing) {
			statsInfo = null;
			stats = null;
		}
	}
}