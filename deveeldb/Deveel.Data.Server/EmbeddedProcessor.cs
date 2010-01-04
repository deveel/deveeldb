using System;

using Deveel.Data.Control;

namespace Deveel.Data.Server {
	public sealed class EmbeddedProcessor : Processor {
		public EmbeddedProcessor(DbController controller, string host_string)
			: base(controller, host_string) {
		}

		private bool closed;

		#region Overrides of Processor

		protected override void SendEvent(byte[] event_msg) {
			//TODO:
		}

		public byte [] Process(byte[] input) {
			return ProcessCommand(input);
		}

		public override void Close() {
			if (!closed) {
				Dispose();
				closed = true;
			}
		}

		public override bool IsClosed {
			get { return closed; }
		}

		#endregion
	}
}