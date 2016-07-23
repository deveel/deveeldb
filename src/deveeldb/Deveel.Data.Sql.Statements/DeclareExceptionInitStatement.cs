using System;
using System.Runtime.Serialization;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DeclareExceptionInitStatement : SqlStatement, IDeclarationStatement {
		public DeclareExceptionInitStatement(string exceptionName, int errorCode) {
			if (String.IsNullOrEmpty(exceptionName))
				throw new ArgumentNullException("exceptionName");

			ExceptionName = exceptionName;
			ErrorCode = errorCode;
		}

		private DeclareExceptionInitStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ExceptionName = info.GetString("ExceptionName");
			ErrorCode = info.GetInt32("ErrorCode");
		}

		public string ExceptionName { get; private set; }

		public int ErrorCode { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			// TODO: Verify that the error code is valid and defined
			context.Request.Context.MapErrorCode(ErrorCode, ExceptionName);
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("ExceptionName", ExceptionName);
			info.AddValue("ErrorCode", ErrorCode);
		}
	}
}
