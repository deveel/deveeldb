using System;
using System.Data;

using Deveel.Data.Client;

namespace Deveel.Data {
	public class DeveelEmbeddedDBDriver : NHibernate.Driver.DriverBase {
		public override IDbConnection CreateConnection() {
			return new DeveelDbConnection();
		}

		public override IDbCommand CreateCommand() {
			return new DeveelDbCommand();
		}

		public override bool UseNamedPrefixInSql {
			get { return true; }
		}

		public override bool UseNamedPrefixInParameter {
			get { return true; }
		}

		public override string NamedPrefix {
			get { return "@"; }
		}
	}
}