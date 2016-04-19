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
using System.IO;

namespace Deveel.Data.Store.Journaled {
	abstract class ResourceBase : IJournaledResource {
		private long id;

		protected ResourceBase(JournaledSystem journaledSystem, long id, string name, IStoreData data) {
			JournaledSystem = journaledSystem;
			this.id = id;
			Name = name;
			Data = data;
		}

		~ResourceBase() {
			Dispose(false);
		}

		protected JournaledSystem JournaledSystem { get; private set; }

		public string Name { get; private set; }

		public bool ReadOnly { get; private set; }

		protected IStoreData Data { get; private set; }

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				if (Data != null)
					Data.Dispose();
			}

			Data = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public long Id {
			get { return id; }
		}

		public int PageSize {
			get { return JournaledSystem.PageSize; }
		}

		public abstract long Size { get; }

		public abstract bool Exists { get; }

		protected void SetReadOnly(bool value) {
			ReadOnly = value;
		}

		public abstract void Read(long pageNumber, byte[] buffer, int offset);

		public abstract void Write(long pageNumber, byte[] buffer, int offset, int count);

		public abstract void SetSize(long value);

		public abstract void Open(bool readOnly);

		public abstract void Close();

		public abstract void Delete();

		protected abstract void Persist(PersistCommand command);

		internal void PersistOpen(bool readOnly) {
			Persist(new PersistOpenCommand(readOnly));
		}

		internal void PersistClose() {
			Persist(new PersistCommand(PersistCommandType.Close));
		}

		internal void PersistDelete() {
			Persist(new PersistCommand(PersistCommandType.Delete));
		}

		internal void PersistSetSize(long newSize) {
			Persist(new PersistSetSizeCommand(newSize));
		}

		internal void PersistPageChange(long page, int offset, int count, Stream source) {
			Persist(new PersistPageChangeCommand(page, offset, count, source));
		}

		internal void Synch() {
			Persist(new PersistCommand(PersistCommandType.Synch));
		}

		internal void OnPostRecover() {
			Persist(new PersistCommand(PersistCommandType.PostRecover));
		}
	}
}
