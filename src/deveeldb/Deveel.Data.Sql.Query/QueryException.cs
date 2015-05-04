using System;

namespace Deveel.Data.Sql.Query {
	// TODO: For the moment just a place-holder
#if !PORTBALE
	[Serializable]
#endif
	public class QueryException : SqlErrorException {
		public QueryException(int errorCode) 
			: base(errorCode) {
		}

		public QueryException(int errorCode, string message) 
			: base(errorCode, message) {
		}

		public QueryException(int errorCode, string message, Exception innerException) 
			: base(errorCode, message, innerException) {
		}
	}
}
