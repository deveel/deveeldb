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
#if !PCL
using System.Security.Cryptography;
#endif

using Deveel.Data.Caching;
using Deveel.Data.Services;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public sealed class QueryContext : Context, IQueryContext {
		/*
#if PCL
		private Random secureRandom;
#else
		private RNGCryptoServiceProvider secureRandom;
#endif
		*/

		internal QueryContext(ISessionContext parentContext)
			: base(parentContext) {

			this.RegisterInstance<IQueryContext>(this);
			/*
#if PCL
			secureRandom = new Random();
#else
			secureRandom = new RNGCryptoServiceProvider();
#endif
			*/

			this.RegisterInstance<ICache>(new MemoryCache(), "TableCache");
		}

		public ISessionContext SessionContext {
			get { return (ISessionContext)ParentContext; }
		}

		protected override string ContextName {
			get { return ContextNames.Query; }
		}

		public IBlockContext CreateBlockContext() {
			return new BlockContext(this);
		}

		/*
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
		*/

	}
}