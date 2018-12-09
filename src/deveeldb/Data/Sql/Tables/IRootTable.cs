using System;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// Marks a table implementation to be the root of analysis of the model
	/// </summary>
	public interface IRootTable : ITable, IEquatable<ITable> {
	}
}