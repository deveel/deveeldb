using System;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public class DeveelDbException : DbException {
		public DeveelDbException()
			: this(null) {
		}

		public DeveelDbException(string message)
			: this(message, null) {
		}

		public DeveelDbException(string message, Exception innerException)
			: base(message, innerException) {
		}

		public override int ErrorCode {
			get {
				if (InnerException is DeveelDbException)
					return ((DeveelDbException) InnerException).ErrorCode;

				return base.ErrorCode;
			}
		}
	}
}
