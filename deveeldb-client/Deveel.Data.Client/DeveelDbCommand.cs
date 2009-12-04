// 
//  DeveelDbCommand.cs
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
	public sealed class DeveelDbCommand : DbCommand {
		public override void Prepare() {
			throw new NotImplementedException();
		}

		public override string CommandText {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override int CommandTimeout {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override CommandType CommandType {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override UpdateRowSource UpdatedRowSource {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		protected override DbConnection DbConnection {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		protected override DbParameterCollection DbParameterCollection {
			get { throw new NotImplementedException(); }
		}

		protected override DbTransaction DbTransaction {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override bool DesignTimeVisible {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override void Cancel() {
			throw new NotImplementedException();
		}

		protected override DbParameter CreateDbParameter() {
			throw new NotImplementedException();
		}

		protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) {
			throw new NotImplementedException();
		}

		public override int ExecuteNonQuery() {
			throw new NotImplementedException();
		}

		public override object ExecuteScalar() {
			throw new NotImplementedException();
		}
	}
}