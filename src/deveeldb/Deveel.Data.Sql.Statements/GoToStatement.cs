using System;
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class GoToStatement : SqlStatement, IPlSqlStatement {
		public GoToStatement(string label) {
			if (String.IsNullOrEmpty(label))
				throw new ArgumentNullException("label");

			Label = label;
		}

		private GoToStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			Label = info.GetString("Label");
		}

		public string Label { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("Label", Label);
		}

		protected override void AppendTo(SqlStringBuilder builder) {
			builder.Append("GOTO ");
			builder.Append("'{0}'", Label);
		}
	}
}
