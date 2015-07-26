using System;
using System.IO;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Statements {
	interface ISqlPreparedStatementBinarySerializer : IObjectSerializer {
		void Serialize(SqlPreparedStatement statement, BinaryWriter writer);

		SqlPreparedStatement Deserialize(BinaryReader reader);
	}
}