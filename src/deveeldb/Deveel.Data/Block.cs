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
using System.Collections.Generic;

using Deveel.Data.Diagnostics;

namespace Deveel.Data {
	public class Block : IBlock {
		private IQuery query;

		public Block(IRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			query = request as IQuery;
			
			Context = request.CreateBlockContext();
			Context.UnregisterService<IBlock>();
			Context.RegisterInstance<IBlock>(this);

			Parent = request as IBlock;

			Access = new RequestAccess(this);
		}

		~Block() {
			Dispose(false);
		}

		public IBlock Parent { get; private set; }

		public IBlock Next { get; set; }

		public IQuery Query {
			get {
				if (query != null)
					return query;

				return Parent.Query;
			}
		}

		public RequestAccess Access { get; private set; }

		IEventSource IEventSource.ParentSource {
			get { return Parent; }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return GetEventMetadata(); }
		}

		protected virtual IEnumerable<KeyValuePair<string, object>> GetEventMetadata() {
			return new KeyValuePair<string, object>[0];
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (Context != null)
					Context.Dispose();
			}

			Context = null;
			query = null;
		}

		public IBlockContext Context { get; private set; }

		IContext IEventSource.Context {
			get { return Context; }
		}

		IBlockContext IRequest.CreateBlockContext() {
			return Context.CreateBlockContext();
		}

		void IBlock.Execute(BlockExecuteContext context) {
			ExecuteBlock(context);
		}

		protected virtual void ExecuteBlock(BlockExecuteContext context) {
			// default implementation is an empty block, that does nothing
		}

		public virtual IBlock CreateBlock() {
			return new Block(this);
		}
	}
}
