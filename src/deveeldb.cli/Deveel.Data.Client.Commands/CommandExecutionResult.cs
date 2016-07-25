using System;

namespace Deveel.Data.Client.Commands {
	public sealed class CommandExecutionResult {
		public CommandExecutionResult(object value) 
			: this(value, value != null) {
		}

		public CommandExecutionResult(object value, bool success) {
			Value = value;
			Success = success;
		}

		public object Value { get; private set; }

		public int ExecutionTime { get; set; }

		public bool Success { get; private set; }

		public Exception Error { get; set; }

		public bool IsPrintable {
			get { return Value is IPrintable; }
		}

		public void PrintTo(IPrintTarget target) {
			if (!(Value is IPrintable) || 
				Value == null)
				return;

			(Value as IPrintable).WriteTo(target);
		}
	}
}
