using System;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SqlUserIdentifier : IPreparable, ISerializable {
		public SqlUserIdentifier(SqlIdentificationType type, SqlExpression argument) {
			Type = type;
			Argument = argument;
		}

		private SqlUserIdentifier(SerializationInfo info, StreamingContext context) {
			Type = (SqlIdentificationType) info.GetInt32("Type");
			Argument = (SqlExpression) info.GetValue("Argument", typeof(SqlExpression));
		}

		public SqlIdentificationType Type { get; private set; }

		public SqlExpression Argument { get; private set; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Type", (int)Type);
			info.AddValue("Argument", Argument, typeof(SqlExpression));
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var arg = Argument.Prepare(preparer);
			return new SqlUserIdentifier(Type, arg);
		}
	}
}
