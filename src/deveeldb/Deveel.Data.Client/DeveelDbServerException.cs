using System;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbServerException : DbException {
		private readonly int errorCode;

		internal DeveelDbServerException(string message, int errorClass, int errorCode)
			: base(message) {
			this.errorCode = errorClass | errorCode;
		}

		public override int ErrorCode {
			get { return errorCode; }
		}
	}
}
