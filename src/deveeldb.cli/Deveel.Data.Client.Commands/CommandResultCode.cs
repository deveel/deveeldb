using System;

namespace Deveel.Data.Client.Commands {
	public enum CommandResultCode {
		Success = 1,
		SyntaxError = 2,
		ExecutionFailed = 3,
	}
}
