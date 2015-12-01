// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Services;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Transactions {
	public class TransactionContext : Context, ITransactionContext	{
		public TransactionContext (IDatabaseContext databaseContext)
			: base(databaseContext) {
			VariableManager = new VariableManager(this);
			CursorManager = new CursorManager(this);
		}

		protected override string ContextName {
			get { return ContextNames.Transaction; }
		}

	    public IDatabaseContext DatabaseContext {
	        get { return (IDatabaseContext) ParentContext; }
	    }

	    public ISessionContext CreateSessionContext() {
			return new SessionContext(this);
		}

		bool ICursorScope.IgnoreCase {
			get { return this.IgnoreIdentifiersCase(); }
		}

		public IVariableManager VariableManager { get; private set; }

		public CursorManager CursorManager { get; private set; }

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (VariableManager != null)
					VariableManager.Dispose();
				if (CursorManager != null)
					CursorManager.Dispose();
			}

			CursorManager = null;
			VariableManager = null;
			base.Dispose(disposing);
		}
	}
}