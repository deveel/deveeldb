// 
//  DeveelDbConnection.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbConnection : DbConnection {
		protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) {
			throw new NotImplementedException();
		}

		public override void Close() {
			throw new NotImplementedException();
		}

		public override void ChangeDatabase(string databaseName) {
			throw new NotImplementedException();
		}

		public override void Open() {
			throw new NotImplementedException();
		}

		public override string ConnectionString {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override string Database {
			get { throw new NotImplementedException(); }
		}

		public override ConnectionState State {
			get { throw new NotImplementedException(); }
		}

		public override string DataSource {
			get { throw new NotImplementedException(); }
		}

		public override string ServerVersion {
			get { throw new NotImplementedException(); }
		}

		protected override DbCommand CreateDbCommand() {
			throw new NotImplementedException();
		}
	}
}