using System;

namespace Deveel.Data.Sql.Statements {
	public interface ILabeledStatement {
		string Label { get; }
	}
}
