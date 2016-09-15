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

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	class SessionTableContainer : ITableContainer {
		private ISession session;

		private static readonly TableInfo[] IntTableInfo;

		private static readonly TableInfo StatisticsTableInfo;

		private static readonly TableInfo SessionInfoTableInfo;

		private static readonly TableInfo OpenSessionsTableInfo;

		public SessionTableContainer(ISession session) {
			this.session = session;
		}

		static SessionTableContainer() {
			// SYSTEM.OPEN_SESSIONS
			OpenSessionsTableInfo = new TableInfo(SystemSchema.OpenSessionsTableName);
			OpenSessionsTableInfo.AddColumn("username", PrimitiveTypes.String());
			OpenSessionsTableInfo.AddColumn("host_string", PrimitiveTypes.String());
			OpenSessionsTableInfo.AddColumn("last_command", PrimitiveTypes.DateTime());
			OpenSessionsTableInfo.AddColumn("time_connected", PrimitiveTypes.DateTime());
			OpenSessionsTableInfo = OpenSessionsTableInfo.AsReadOnly();

			// SYSTEM.SESSION_INFO
			SessionInfoTableInfo = new TableInfo(SystemSchema.SessionInfoTableName);
			SessionInfoTableInfo.AddColumn("var", PrimitiveTypes.String());
			SessionInfoTableInfo.AddColumn("value", PrimitiveTypes.String());
			SessionInfoTableInfo = SessionInfoTableInfo.AsReadOnly();

			// SYSTEM.STATS
			StatisticsTableInfo = new TableInfo(SystemSchema.StatisticsTableName);
			StatisticsTableInfo.AddColumn("stat_name", PrimitiveTypes.String());
			StatisticsTableInfo.AddColumn("value", PrimitiveTypes.String());
			StatisticsTableInfo = StatisticsTableInfo.AsReadOnly();

			IntTableInfo = new TableInfo[3];
			IntTableInfo[0] = StatisticsTableInfo;
			IntTableInfo[1] = SessionInfoTableInfo;
			IntTableInfo[2] = OpenSessionsTableInfo;
		}

		public int TableCount {
			get { return IntTableInfo.Length; }
		}

		public int FindByName(ObjectName name) {
			var ignoreCase = session.IgnoreIdentifiersCase();
			for (int i = 0; i < IntTableInfo.Length; i++) {
				var info = IntTableInfo[i];
				if (info != null &&
					info.TableName.Equals(name, ignoreCase))
					return i;
			}

			return -1;
		}

		public ObjectName GetTableName(int offset) {
			if (offset < 0 || offset >= IntTableInfo.Length)
				throw new ArgumentOutOfRangeException("offset");

			return IntTableInfo[offset].TableName;
		}

		public TableInfo GetTableInfo(int offset) {
			if (offset < 0 || offset >= IntTableInfo.Length)
				throw new ArgumentOutOfRangeException("offset");

			return IntTableInfo[offset];
		}

		public string GetTableType(int offset) {
			return TableTypes.SystemTable;
		}

		public bool ContainsTable(ObjectName name) {
			return FindByName(name) != -1;
		}

		public ITable GetTable(int offset) {
			switch (offset) {
				case 0:
					return new StatisticsTable(session);
				case 1:
					return new SessionInfoTable(session);
				case 2:
					return new OpenSessionsTable(session);
				default:
					throw new ArgumentOutOfRangeException("offset");
			}
		}

		#region OpenSessionsTable

		private class OpenSessionsTable : GeneratedTable {
			private ISession session;

			public OpenSessionsTable(ISession session)
				: base(session.Context) {
				this.session = session;
			}

			public override TableInfo TableInfo {
				get { return OpenSessionsTableInfo; }
			}

			public override int RowCount {
				get { return session.Database().Sessions.Count; }
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= session.Database().Sessions.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var openSession = session.Database().Sessions[(int)rowNumber];
				var lastCommandTime = (SqlDateTime)session.LastCommandTime();

				switch (columnOffset) {
					case 0:
						return GetColumnValue(0, new SqlString(openSession.User.Name));
					case 1:
						return GetColumnValue(1, SqlString.Null);
					case 2:
						return GetColumnValue(2, lastCommandTime);
					case 3:
						return GetColumnValue(3, (SqlDateTime)openSession.StartedOn());
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}

			protected override void Dispose(bool disposing) {
				session = null;
				base.Dispose(disposing);
			}
		}

		#endregion

		#region SessionInfoTable

		class SessionInfoTable : GeneratedTable {
			public SessionInfoTable(ISession session)
				: base(session.Context) {
			}

			public override TableInfo TableInfo {
				get { throw new NotImplementedException(); }
			}

			public override int RowCount {
				get { throw new NotImplementedException(); }
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region StatisticsTable

		class StatisticsTable : GeneratedTable {
			private ISession session;
			private List<Counter> stats;

			public StatisticsTable(ISession session)
				: base(session.Context) {
				this.session = session;
				Init();
			}

			public override TableInfo TableInfo {
				get { return StatisticsTableInfo; }
			}

			public override int RowCount {
				get { return stats.Count; }
			}

			private void Init() {
				lock (session) {
					stats = session.Database().Counters.ToList();
				}
			}

			public override Field GetValue(long rowNumber, int columnOffset) {
				if (rowNumber < 0 || rowNumber >= stats.Count)
					throw new ArgumentOutOfRangeException("rowNumber");

				var counter = stats[(int) rowNumber];

				switch (columnOffset) {
					case 0:
						return GetColumnValue(columnOffset, new SqlString(counter.Name));
					case 1:
						return GetColumnValue(columnOffset, counter.ValueAsNumber());
					default:
						throw new ArgumentOutOfRangeException("columnOffset");
				}
			}

			protected override void Dispose(bool disposing) {
				if (disposing) {
					if (stats != null)
						stats.Clear();
				}

				stats = null;
				session = null;
				base.Dispose(disposing);
			}
		}

		#endregion
	}
}
