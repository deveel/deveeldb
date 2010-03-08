// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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