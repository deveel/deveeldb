using System;

namespace Deveel.Data.Sql.Statements {
	interface ILabeledExecutable : IExecutable {
		string Label { get; }
	}
}
