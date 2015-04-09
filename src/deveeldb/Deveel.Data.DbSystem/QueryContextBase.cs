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
using System.Security.Cryptography;

using Deveel.Data.Caching;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.DbSystem {
	public abstract class QueryContextBase : IQueryContext {
		private readonly RNGCryptoServiceProvider secureRandom;
		private readonly ICache tableCache;
		private bool disposed;

		protected QueryContextBase() {
			secureRandom = new RNGCryptoServiceProvider();
			tableCache = new MemoryCache(512, 1024, 30);
		}

		~QueryContextBase() {
			Dispose(false);
		}


		public ISystemContext SystemContext {
			get { return DatabaseContext.SystemContext; }
		}

		public abstract IDatabaseContext DatabaseContext { get; }

		public abstract IUserSession Session { get; }

		public virtual ICache TableCache {
			get { return tableCache; }
		}

		public virtual SqlNumber NextRandom(int bitSize) {
			var bytes = new byte[8];
			secureRandom.GetBytes(bytes);
			var num = BitConverter.ToInt64(bytes, 0);
			return new SqlNumber(num);
		}

		public void Dispose() {
			if (disposed)
				return;

			try {
				Dispose(true);
			} finally {
				disposed = true;
			}
			
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
		}
	}
}