using System;
using System.IO;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements.Blocks {
	public interface IStatementBlockSerializer : IObjectSerializer {
		StatementBlock DeserializeBlock(BinaryReader reader);
	}
}
