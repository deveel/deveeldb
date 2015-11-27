using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql {
	public sealed class ExecutionContext : IDisposable {
		public ExecutionContext(IRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			Request = request;
		}

		public IRequest Request { get; private set; }

		public ITable Result { get; private set; }

		public bool HasResult { get; private set; }

		public bool HasTermination { get; private set; }

		public void SetResult(ITable result) {
			if (result != null) {
				Result = result;
				HasResult = true;
			}
		}

		public void Terminate() {
			HasTermination = true;
		}

		public void Dispose() {
			Request = null;
		}
	}
}
