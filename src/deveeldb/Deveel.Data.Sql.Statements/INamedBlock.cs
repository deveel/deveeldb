using System;

namespace Deveel.Data.Sql.Statements {
	interface INamedBlock : IBlock {
		string Name { get; }
	}
}
