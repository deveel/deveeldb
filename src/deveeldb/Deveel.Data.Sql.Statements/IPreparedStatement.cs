using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	public interface IPreparedStatement : IExecutable, ISerializable {
	}
}