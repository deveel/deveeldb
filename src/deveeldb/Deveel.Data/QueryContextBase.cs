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
using System.Collections;
#if !PCL
using System.Security.Cryptography;
#endif

using Deveel.Data.Caching;
using Deveel.Data.Services;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data {
	abstract class QueryContextBase : Context, IQueryContext {
#if PCL
		private Random secureRandom;
#else
		private RNGCryptoServiceProvider secureRandom;
#endif
		private bool disposed;

		protected QueryContextBase(IUserSession session)
			: base(session.SessionContext) {
			if (session == null)
				throw new ArgumentNullException("session");

			this.RegisterInstance<IQueryContext>(this);
#if PCL
			secureRandom = new Random();
#else
			secureRandom = new RNGCryptoServiceProvider();
#endif
			Session = session;

			this.RegisterInstance<ICache>(new MemoryCache(), "TableCache");
		}

		IQueryContext IQueryContext.ParentContext {
			get { return ParentQueryContext; }
		}
		public virtual IQueryContext ParentQueryContext {
			get { return null; }
		}

	    public ISessionContext SessionContext {
	        get { return Session.SessionContext; }
	    }


		public CursorManager CursorManager { get; private set; }

		public IUserSession Session {get; private set; }

		public virtual string CurrentSchema {
			get { return Session.CurrentSchema; }
		}

		protected override string ContextName {
			get { return ContextNames.Query;  }
		}

		private void AssertNotDisposed() {
			if (disposed)
				throw new ObjectDisposedException("QueryContext", "The query context was disposed.");
		}

		public virtual SqlNumber NextRandom(int bitSize) {
			AssertNotDisposed();

#if PCL
			var num = secureRandom.NextDouble();
#else
			var bytes = new byte[8];
			secureRandom.GetBytes(bytes);
			var num = BitConverter.ToInt64(bytes, 0);
#endif
			return new SqlNumber(num);
		}

		protected override void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					if (CursorManager != null)
						CursorManager.Dispose();
				}

				CursorManager = null;
				secureRandom = null;
				Session = null;

				disposed = true;
			}

			base.Dispose(true);
		}
	}
}