using System;
using System.IO;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	abstract class SqlPreparedStatementSerializer<TStatement> : ISqlPreparedStatementBinarySerializer where TStatement : SqlPreparedStatement {
		public abstract void Serialize(TStatement statement, BinaryWriter writer);

		public abstract TStatement Deserialize(BinaryReader reader);

		void IObjectSerializer.Serialize(object obj, Stream outputStream) {
			using (var writer = new BinaryWriter(outputStream, Encoding.Unicode)) {
				Serialize((TStatement)obj, writer);
			}
		}

		object IObjectSerializer.Deserialize(Stream inputStream) {
			using (var reader = new BinaryReader(inputStream, Encoding.Unicode)) {
				return Deserialize(reader);
			}
		}

		void ISqlPreparedStatementBinarySerializer.Serialize(SqlPreparedStatement statement, BinaryWriter writer) {
			Serialize((TStatement)statement, writer);
		}

		SqlPreparedStatement ISqlPreparedStatementBinarySerializer.Deserialize(BinaryReader reader) {
			return Deserialize(reader);
		}
	}
}