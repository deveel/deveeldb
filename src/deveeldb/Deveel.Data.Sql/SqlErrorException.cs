using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql {
	[Serializable]
	public class SqlErrorException : ErrorException {
		public SqlErrorException(int errorCode) 
			: base(EventClasses.SqlModel, errorCode) {
		}

		public SqlErrorException(int errorCode, string message) 
			: base(EventClasses.SqlModel, errorCode, message) {
		}

		public SqlErrorException(int errorCode, string message, Exception innerException) 
			: base(EventClasses.SqlModel, errorCode, message, innerException) {
		}
	}
}
