using System;

namespace Deveel.Data.Sql.Statements.Blocks {
	interface INamedBlock : IBlock {
		string Name { get; }
	}
}
