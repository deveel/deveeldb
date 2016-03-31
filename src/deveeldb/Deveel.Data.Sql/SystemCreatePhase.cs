using System;

namespace Deveel.Data.Sql {
	public enum SystemCreatePhase {
		SystemCreate = 1,
		SystemSetup = 2,
		DatabaseCreate = 3,
		DatabaseCreated = 4
	}
}
