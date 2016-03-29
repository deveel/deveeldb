// 
//  Copyright 2010-2016 Deveel
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
//


using System;

using Deveel.Data.Diagnostics;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public class SessionContext : Context, ISessionContext {
		public SessionContext(ITransactionContext transactionContext)
			: base(transactionContext) {
			EventRegistry = new EventRegistry(this);
		}

		public ITransactionContext TransactionContext {
			get { return (ITransactionContext)ParentContext; }
		}

		protected override string ContextName {
			get { return ContextNames.Session; }
		}


		public EventRegistry EventRegistry { get; private set; }

		IEventRegistry IEventScope.EventRegistry {
			get { return EventRegistry; }
		}
		
		public IQueryContext CreateQueryContext() {
			return new QueryContext(this);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (EventRegistry != null)
					EventRegistry.Dispose();
			}

			EventRegistry = null;
			base.Dispose(disposing);
		}
	}
}

