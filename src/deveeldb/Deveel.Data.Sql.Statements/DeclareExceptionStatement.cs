using System;

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DeclareExceptionStatement : SqlStatement {
		public DeclareExceptionStatement(string exceptionName) {
			if (String.IsNullOrEmpty(exceptionName))
				throw new ArgumentNullException("exceptionName");

			ExceptionName = exceptionName;
		}

		private DeclareExceptionStatement(ObjectData data) {
			ExceptionName = data.GetString("Exception");
		}

		public string ExceptionName { get; private set; }

		protected override void GetData(SerializeData data) {
			data.SetValue("Exception", ExceptionName);
		}
	}
}